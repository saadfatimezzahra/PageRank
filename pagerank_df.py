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

def main(input_path, num_partitions, output):
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
    edges = edges.repartition(num_partitions).cache()
    
    # Calcul des degrés sortants (OutDegree) pour chaque source
    # Utile pour diviser le rank par le nombre de voisins
    out_degrees = edges.groupBy("src").agg(F.count("dst").alias("out_degree"))
    
    # Univers complet des pages (src + dst) pour ne perdre personne
    # C'est crucial pour réintégrer les pages qui ne font que recevoir (sinks)
    all_nodes = edges.select("src").union(edges.select("dst")).distinct().withColumnRenamed("src", "id")
    all_nodes = all_nodes.repartition(num_partitions).cache()
    
    N = all_nodes.count()
    print(f"Nombre total de pages (N) : {N}")

    # 3. Initialisation des Ranks (1.0 pour tout le monde)
    ranks = all_nodes.withColumn("rank", F.lit(1.0))

    print("\n" + "="*100)
    print(f"{'DÉBUT DU CALCUL PAGERANK (DataFrame)':^100}")
    print("="*100)

    global_start = time.time()

    for i in range(ITERATIONS):
        print(f"\n--- Itération {i+1} ---")
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
    

    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        fs.delete(path, True)
    
    # Affichage final Top 10
    ranks.orderBy(F.col("rank").desc()).limit(10).coalesce(1).write.csv(output, header=True)
    total_time = time.time() - global_start
    print(f"\nTemps Total : {total_time:.2f}s")
    

# ================= AFFICHAGE (Même logique que RDD) =================
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python pagerank_df.py <data file> <num partitions> <output_path>")
    else:
        filepath = sys.argv[1] # string
        num_partitions = int(sys.argv[2]) # Regle: 2 a 4 x nombre de coeurs de la machine
        output = sys.argv[3]
        main(filepath, num_partitions, output)