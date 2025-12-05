from pyspark.sql import SparkSession
from operator import add
import time
import sys

# ==========================================
# CONFIGURATION
# ==========================================
DAMPING = 0.85
ITERATIONS = 10
NUM_PARTITIONS = 4  # Adapté pour 2 cores (2 à 4 partitions)

def main(input_path):
    # 1. Init Spark
    spark = SparkSession.builder \
        .appName("PageRank-NSDI-Visual") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR") # Pour moins de bruit dans la console

    print(f"Lecture des données depuis : {input_path}")

    # 2. Parsing (Format TTL : <src> <predicate> <dst> .)
    def parse_ttl(line):
        parts = line.split(" ")
        if len(parts) < 3: return None
        # On nettoie les chevrons < >
        src = parts[0].strip('<>') 
        dst = parts[2].strip('<>')
        return (src, dst)

    lines = sc.textFile(input_path, minPartitions=NUM_PARTITIONS)
    edges = lines.map(parse_ttl).filter(lambda x: x is not None)

    # 3. Optimisation NSDI : Création de la structure statique (Links)
    # partitionBy est LA clé pour éviter le shuffle répété
    links = edges.distinct().groupByKey().partitionBy(NUM_PARTITIONS).cache()
    
    # Force le calcul pour mettre en cache et compter les noeuds
    N = links.count()
    print(f"Nombre de pages uniques (N) : {N}")

    # 4. Initialisation des Ranks
    # mapValues préserve le partitionneur de 'links' -> Ranks et Links sont alignés
    ranks = links.mapValues(lambda _: 1.0)

    print("\n" + "="*80)
    print(f"{'DÉBUT DU CALCUL PAGERANK':^80}")
    print("="*80 + "\n")

    global_start = time.time()

    # ==========================================
    # BOUCLE D'ITÉRATIONS
    # ==========================================
    for i in range(ITERATIONS):
        iter_start = time.time()

        # A. Calcul des contributions (Local Join grâce au partitionnement)
        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )

        # B. Somme des contributions reçues
        contribs_sum = contribs.reduceByKey(add)

        # C. Calcul de la masse perdue (Dangling Nodes)
        # Somme actuelle des ranks. Ce qui manque pour arriver à N est perdu.
        current_mass = ranks.values().sum()
        dangling_mass = N - current_mass

        # D. Mise à jour des ranks (Full Join pour ne perdre personne)
        # On utilise links comme base pour garder tous les noeuds
        ranks = links.leftOuterJoin(contribs_sum).mapValues(
            lambda x: update_rank(x[1], dangling_mass, N)
        )
        
        # E. ACTION & AFFICHAGE
        # On récupère le Top 5 pour l'afficher
        # Cela force aussi le calcul de l'itération (Action)
        top_5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        
        iter_duration = time.time() - iter_start
        
        # Affichage du tableau pour cette itération
        print_iteration_table(i + 1, iter_duration, top_5)

    total_time = time.time() - global_start

    # ==========================================
    # RÉSULTAT FINAL
    # ==========================================
    print("\n" + "="*80)
    print(f"{'CALCUL TERMINÉ':^80}")
    print("="*80)
    print(f"Temps Total : {total_time:.2f} secondes")
    
    # Récupérer le Top 10 final
    final_top_10 = ranks.takeOrdered(10, key=lambda x: -x[1])
    print_final_table(final_top_10)

    spark.stop()

# ==========================================
# FONCTIONS LOGIQUES
# ==========================================
def compute_contribs(urls, rank):
    """Distribue le rank aux voisins."""
    num_urls = len(urls)
    if num_urls > 0:
        val = rank / num_urls
        for url in urls:
            yield (url, val)

def update_rank(sum_contribs, dangling_mass, N):
    """Formule PageRank avec redistribution de la masse perdue."""
    s = sum_contribs if sum_contribs is not None else 0.0
    # Formule standard: 0.15 + 0.85 * (Contribution + Redistribution)
    return 0.15 + 0.85 * (s + dangling_mass / N)

# ==========================================
# FONCTIONS D'AFFICHAGE (TABLEAUX)
# ==========================================
def print_iteration_table(iteration, duration, rows):
    print(f"\n>>> ITÉRATION {iteration} (Durée : {duration:.3f}s)")
    print("-" * 85)
    # En-tête du tableau
    print(f"| {'#':<3} | {'Page URL (Top 5)':<60} | {'Score':>12} |")
    print("-" * 85)
    
    for idx, (url, score) in enumerate(rows, 1):
        # On tronque l'URL si elle est trop longue pour l'affichage
        display_url = (url[:57] + '..') if len(url) > 59 else url
        print(f"| {idx:<3} | {display_url:<60} | {score:12.6f} |")
    
    print("-" * 85)

def print_final_table(rows):
    print("\n")
    print("+" + "="*78 + "+")
    print(f"|{'TOP 10 RESULTATS FINAUX':^78}|")
    print("+" + "="*78 + "+")
    print(f"| {'#':<3} | {'Page URL':<55} | {'Final Score':>12} |")
    print("|" + "-"*78 + "|")
    
    for idx, (url, score) in enumerate(rows, 1):
        display_url = (url[:52] + '...') if len(url) > 55 else url
        print(f"| {idx:<3} | {display_url:<55} | {score:12.6f} |")
    
    print("+" + "="*78 + "+")
    print("\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pagerank_table.py <file_path>")
    else:
        main(sys.argv[1])