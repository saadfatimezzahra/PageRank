from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import time
import sys

# ==========================================
# CONFIGURATION
# ==========================================
DAMPING = 0.85
ITERATIONS = 10
# Nombre de partitions pour le shuffle (ajuster selon ta machine, 4 pour 2 cores c'est bien)
NUM_PARTITIONS = 4 

def main(input_path):
    spark = SparkSession.builder \
        .appName("PageRank-DataFrame") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"--- Lecture de : {input_path} ---")

    # 1. Parsing avec DataFrame (Lecture texte brut puis split)
    # On lit en text
    raw_df = spark.read.text(input_path)

    # On extrait src et dst via split ou regex
    # Format: <src> <pred> <dst> .
    # On suppose que les champs sont séparés par des espaces
    split_col = F.split(raw_df['value'], ' ')
    
    # Filtre basique pour ignorer les lignes malformées
    edges = raw_df.select(
        F.trim(F.regexp_replace(split_col.getItem(0), "[<>]", "")).alias("src"),
        F.trim(F.regexp_replace(split_col.getItem(2), "[<>]", "")).alias("dst")
    ).filter(F.length(F.col("src")) > 0).filter(F.length(F.col("dst")) > 0)

    # 2. Préparation du Graphe
    # Contrairement aux RDD, on garde souvent le format tabulaire (Edge List) pour les joins
    
    # On met en cache les arêtes car elles servent à chaque tour
    edges = edges.repartition(NUM_PARTITIONS).cache()
    
    # Calcul des degrés sortants (OutDegree) pour chaque source
    # Utile pour diviser le rank par le nombre de voisins
    out_degrees = edges.groupBy("src").agg(F.count("dst").alias("out_degree"))
    
    # Univers complet des pages (src + dst) pour ne perdre personne
    # C'est crucial pour réintégrer les pages qui ne font que recevoir (sinks)
    all_nodes = edges.select("src").union(edges.select("dst")).distinct().withColumnRenamed("src", "id")
    all_nodes = all_nodes.repartition(NUM_PARTITIONS).cache()
    
    N = all_nodes.count()
    print(f"Nombre total de pages (N) : {N}")

    # 3. Initialisation des Ranks (1.0 pour tout le monde)
    ranks = all_nodes.withColumn("rank", F.lit(1.0))

    print("\n" + "="*100)
    print(f"{'DÉBUT DU CALCUL PAGERANK (DataFrame)':^100}")
    print("="*100)

    global_start = time.time()

    for i in range(ITERATIONS):
        iter_start = time.time()

        # --- A. Préparation des contributions ---
        # On joint les ranks actuels avec les edges pour savoir qui donne combien à qui.
        # Mais d'abord, on joint edges avec out_degrees pour avoir le diviseur.
        # (On pourrait pré-calculer edges_with_degree pour optimiser encore plus)
        
        # sources_with_rank : [src, rank, out_degree]
        sources_with_rank = ranks.join(out_degrees, ranks.id == out_degrees.src, "inner") \
             .select(ranks.id, ranks.rank, out_degrees.out_degree)

        # contributions : [dst, contrib]
        # contrib = rank / out_degree
        contribs = edges.join(sources_with_rank, edges.src == sources_with_rank.id) \
            .select(edges.dst, (F.col("rank") / F.col("out_degree")).alias("contrib"))

        # --- B. Calcul de la masse perdue (Dangling Mass) ---
        # La somme des ranks actuels avant mise à jour.
        # Attention: C'est la somme des ranks de l'itération *précédente* qui compte pour la perte.
        # Mais ici, on utilise l'astuce : Masse Totale Théorique - Masse Transmise = Masse Perdue.
        # Masse Transmise = somme(contribs)
        
        # Pour faire simple et robuste comme dans la version RDD :
        # On calcule la masse présente dans le système au début du tour (c'est N si on part de 1.0)
        # Mais à l'itération i, la masse "transmise" est la somme de toutes les contributions calculées.
        # Tout ce qui manque par rapport à N est ce qui est tombé dans des puits.
        
        total_transmitted_mass = contribs.agg(F.sum("contrib")).collect()[0][0]
        if total_transmitted_mass is None: total_transmitted_mass = 0.0
        
        # Puisque la somme totale des ranks doit toujours être N (si on initialise à 1.0)
        # La masse perdue est N - masse_transmise
        dangling_mass = N - total_transmitted_mass

        # --- C. Agrégation et Mise à jour ---
        # Somme des contributions reçues par destination
        sum_contribs = contribs.groupBy("dst").agg(F.sum("contrib").alias("sum_contrib"))

        # On rejoint avec l'univers complet (all_nodes) pour récupérer ceux qui ont 0 contributions
        # Formule : 0.15 + 0.85 * (sum_contrib + dangling_mass / N)
        
        new_ranks = all_nodes.join(sum_contribs, all_nodes.id == sum_contribs.dst, "left_outer") \
            .select(
                F.col("id"),
                F.coalesce(F.col("sum_contrib"), F.lit(0.0)).alias("received")
            ) \
            .withColumn(
                "rank", 
                0.15 + 0.85 * (F.col("received") + (dangling_mass / N))
            ).select("id", "rank")

        # --- D. CASSER LE LIGNAGE (CRUCIAL) ---
        # Sans ça, Spark plante après quelques itérations complexes.
        # localCheckpoint coupe le plan d'exécution et stocke les données sur le disque local des exécuteurs.
        # Si localCheckpoint plante (manque d'espace), utiliser .cache() + .count()
        ranks = new_ranks.localCheckpoint() 
        
        # Action pour forcer le calcul immédiat
        # On récupère le top 5 pour l'affichage
        top5 = ranks.orderBy(F.col("rank").desc()).limit(5).collect()
        
        dt = time.time() - iter_start
        print_iter(i+1, dt, top5)

    total_time = time.time() - global_start
    print(f"\nTemps Total : {total_time:.2f}s")

    # Affichage final Top 10
    top10 = ranks.orderBy(F.col("rank").desc()).limit(10).collect()
    print_final(top10)
    
    spark.stop()

# ================= AFFICHAGE (Même logique que RDD) =================

def print_iter(i, dt, rows):
    print(f"\n> Iter {i} ({dt:.2f}s)")
    print("-" * 120)
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    for idx, row in enumerate(rows, 1):
        url = row['id']
        r = row['rank']
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")

def print_final(rows):
    print("\n" + "="*120)
    print(f"{'RESULTATS FINAUX':^120}")
    print("="*120)
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    for idx, row in enumerate(rows, 1):
        url = row['id']
        r = row['rank']
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")
    print("="*120 + "\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pagerank_df.py data.ttl")
    else:
        main(sys.argv[1])