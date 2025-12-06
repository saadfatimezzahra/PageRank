from pyspark.sql import SparkSession
from operator import add
import sys
import time

# ==========================================
# CONFIGURATION
# ==========================================
DAMPING = 0.85
ITERATIONS = 10


# ==========================================
# PROGRAMME PRINCIPAL
# ==========================================
def main(input_path, num_partitions, output_path):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-Final") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"--- Lecture de : {input_path} ---")
    print(f"--- Nombre de partitions : {num_partitions} ---")

    # ----------------------------
    # 1. PARSING TTL
    # ----------------------------
    def parse_ttl(line):
        parts = line.split(" ")
        if len(parts) < 3:
            return None
        src = parts[0].strip("<>")
        dst = parts[2].strip("<>")
        return (src, dst)

    raw_data = sc.textFile(input_path, minPartitions=num_partitions)
    edges = raw_data.map(parse_ttl).filter(lambda x: x is not None)

    # ----------------------------
    # 2. STRUCTURE DU GRAPHE
    # ----------------------------
    out_links = edges.distinct().groupByKey()
    all_pages = edges.flatMap(lambda x: x).distinct()

    links = all_pages.map(lambda p: (p, 0)) \
        .leftOuterJoin(out_links) \
        .mapValues(lambda x: x[1] if x[1] is not None else []) \
        .partitionBy(num_partitions) \
        .cache()

    N = links.count()
    print(f"Nombre total de pages (N) : {N}")

    # ----------------------------
    # 3. INITIALISATION DES RANKS
    # ----------------------------
    ranks = links.mapValues(lambda _: 1.0)

    print("\n" + "=" * 120)
    print(f"{'DEBUT DU CALCUL PAGERANK (RDD)':^120}")
    print("=" * 120)

    global_start = time.time()

    # ----------------------------
    # 4. BOUCLE ITERATIVE PAGERANK
    # ----------------------------
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

        top5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        dt = time.time() - iter_start
        print_iter(i + 1, dt, top5)

    total_time = time.time() - global_start
    print(f"\nTemps TOTAL PageRank : {total_time:.3f} sec\n")

    # ----------------------------
    # 5. EXPORT DES RESULTATS
    # ----------------------------
    top10 = ranks.takeOrdered(10, key=lambda x: -x[1])

    print_final(top10)
    save_results(spark, top10, output_path)

    print(f"\nRésultat exporté dans : {output_path}\n")

    spark.stop()


# ==========================================
# LOGIQUE PAGERANK
# ==========================================
def compute_contribs(urls, rank):
    n = len(urls)
    if n > 0:
        val = rank / n
        for url in urls:
            yield (url, val)

def update_rank(sum_contribs, dangling_mass, N):
    received = sum_contribs if sum_contribs else 0.0
    return 0.15 + 0.85 * (received + dangling_mass / N)


# ==========================================
# AFFICHAGE
# ==========================================
def print_iter(i, dt, rows):
    print(f"\n> Iteration {i} ({dt:.2f}s)")
    print("-" * 120)
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    for idx, (url, r) in enumerate(rows, 1):
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")

def print_final(rows):
    print("\n" + "=" * 120)
    print(f"{'RESULTATS FINAUX TOP 10':^120}")
    print("=" * 120)
    print(f"| {'#':<3} | {'Page URL':<90} | {'Rank':>12} |")
    print("-" * 120)
    for idx, (url, r) in enumerate(rows, 1):
        print(f"| {idx:<3} | {url:<90} | {r:12.6f} |")
    print("=" * 120)


# ==========================================
# EXPORT CSV POUR CLOUD / README
# ==========================================
def save_results(spark, rows, output_path):
    df = spark.createDataFrame(rows, ["page", "rank"])
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: spark-submit pagerank_rdd.py <file.ttl> <num_partitions> <output_path>")
        sys.exit(1)

    input_file = sys.argv[1]
    num_partitions = int(sys.argv[2])
    output_path = sys.argv[3]

    main(input_file, num_partitions, output_path)
