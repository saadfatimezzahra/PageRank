from pyspark.sql import SparkSession
from operator import add
import sys
import time

# ==========================================
# CONFIGURATION
# ==========================================
DAMPING = 0.85
ITERATIONS = 10
NUM_PARTITIONS = 4  

def main(input_path):
    spark = SparkSession.builder \
        .appName("PageRank-Final-FullDisplay") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"--- Lecture de : {input_path} ---")

    # 1. Parsing
    def parse_ttl(line):
        parts = line.split(" ")
        if len(parts) < 3: return None
        src = parts[0].strip('<>') 
        dst = parts[2].strip('<>')
        return (src, dst)

    raw_data = sc.textFile(input_path, minPartitions=NUM_PARTITIONS)
    edges = raw_data.map(parse_ttl).filter(lambda x: x is not None)

    # 2. Construction Univers Complet (Optimisation NSDI)
    out_links = edges.distinct().groupByKey()
    all_pages = edges.flatMap(lambda x: x).distinct()
    
    links = all_pages.map(lambda p: (p, 0)) \
        .leftOuterJoin(out_links) \
        .mapValues(lambda x: x[1] if x[1] is not None else []) \
        .partitionBy(NUM_PARTITIONS) \
        .cache()

    N = links.count()
    print(f"Nombre total de pages (N) : {N}")

    # 3. Initialisation
    ranks = links.mapValues(lambda _: 1.0)

    print("\n" + "="*100)
    print(f"{'DÉBUT DU CALCUL PAGERANK':^100}")
    print("="*100)

    global_start = time.time()

    for i in range(ITERATIONS):
        iter_start = time.time()

        current_mass = ranks.values().sum()
        dangling_mass = N - current_mass

        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )

        contribs_sum = contribs.reduceByKey(add)

        ranks = links.leftOuterJoin(contribs_sum).mapValues(
            lambda x: update_rank(x[1], dangling_mass, N)
        )
        
        # Action & Affichage
        top5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        
        dt = time.time() - iter_start
        print_iter(i+1, dt, top5)

    total_time = time.time() - global_start
    print(f"\nTemps Total : {total_time:.2f}s")

    # Affichage final
    top10 = ranks.takeOrdered(10, key=lambda x: -x[1])
    print_final(top10)
    
    spark.stop()


# ================= LOGIQUE MÉTIER =================

def compute_contribs(urls, rank):
    num_urls = len(urls)
    if num_urls > 0:
        val = rank / num_urls
        for url in urls:
            yield (url, val)

def update_rank(sum_contribs, dangling_mass, N):
    received = sum_contribs if sum_contribs is not None else 0.0
    return 0.15 + 0.85 * (received + dangling_mass / N)


# ================= AFFICHAGE MODIFIÉ =================

def print_iter(i, dt, rows):
    print(f"\n> Iter {i} ({dt:.2f}s)")
    # J'augmente la largeur du trait pour accommoder les longues URLs
    print("-" * 120)
    # Colonne Page URL large (90 chars), et "Rank" au lieu de "Score"
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    
    for idx, (url, r) in enumerate(rows, 1):
        # PLUS DE TRONCATURE ICI. On affiche l'URL complète.
        # Si l'URL dépasse 90 caractères, elle poussera juste la colonne, c'est ok.
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")

def print_final(rows):
    print("\n" + "="*120)
    print(f"{'RESULTATS FINAUX':^120}")
    print("="*120)
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    
    for idx, (url, r) in enumerate(rows, 1):
        # Affiche l'URL complète
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")
    
    print("="*120 + "\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pagerank_final.py data.ttl")
    else:
        main(sys.argv[1])