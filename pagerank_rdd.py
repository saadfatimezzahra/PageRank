from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10


# ===============================
# 1. Lecture TTL (simple sample)
# ===============================
def read_ttl(sc, input_path):
    rdd = sc.textFile(input_path)

    def parse(line):
        parts = line.split(" ")
        if len(parts) < 3:
            return None
        src = parts[0][1:-1]
        dst = parts[2][1:-1]
        return (src, dst)

    return rdd.map(lambda l: l.strip()) \
              .filter(lambda l: len(l) > 0) \
              .map(parse) \
              .filter(lambda x: x is not None)


# ===============================
# 2. Display Helper
# ===============================
def pretty(title, rows):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    if not rows:
        print("(vide)")
        return
    col = max(len(r[0]) for r in rows)
    for page, rank in rows:
        print(f"{page:<{col}}  |  {rank:.6f}")
    print("=" * 70)


# ===============================
# 3. MAIN PAGERANK (RDD NSDI)
# ===============================
def main(path):

    spark = SparkSession.builder.appName("PageRank-RDD-NSDI").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nLecture :", path)
    edges = read_ttl(sc, path).cache()
    print("Edges :", edges.count())

    # Ensemble des pages
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()
    N = pages.count()
    print("Pages :", N)

    # ============================
    #  A. Construire adjacency list
    # ============================
    raw_links = edges.groupByKey().mapValues(list)

    # Tous les noeuds doivent exister (même degree=0)
    links = (
        pages.map(lambda p: (p, []))
             .leftOuterJoin(raw_links)
             .mapValues(lambda x: x[1] if x[1] else [])
             .cache()
    )

    # ============================
    #  B. Fixer partitionnement
    # ============================
    PARTS = links.getNumPartitions()
    links = links.partitionBy(PARTS).cache()

    # Initialiser ranks SUR LE MÊME partitionneur
    ranks = links.mapValues(lambda _: 1.0).cache()

    # Degrés sortants (fixe)
    out_deg = links.mapValues(len).cache()

    # Nœuds sans sorties (dangling)
    dangling_nodes = out_deg.filter(lambda x: x[1] == 0).map(lambda x: x[0]).cache()

    base = (1 - DAMPING) / N

    print("\n===== Début PageRank NSDI =====\n")

    global_start = time.time()

    for i in range(ITERATIONS):
        iter_start = time.time()

        # Rank total des dangling nodes
        dangling_rank = ranks.join(dangling_nodes.map(lambda x: (x, None))) \
                             .map(lambda x: x[1][0]) \
                             .sum()

        # Contributions
        contribs = (
            links.join(ranks)  # local join → pas de shuffle
                 .flatMap(lambda x:
                    [(dst, x[1][1] / len(x[1][0])) for dst in x[1][0]]  # distribute rank
                 )
        )

        # Nouveau ranks
        ranks = (
            contribs.reduceByKey(add)
                    .mapValues(lambda s: base + DAMPING * (s + dangling_rank / N))
                    .partitionBy(PARTS)
                    .cache()
        )

        iter_end = time.time()

        print(f"Iteration {i+1} terminée en {iter_end - iter_start:.3f} sec.")

        top5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        pretty(f"Top 5 - iteration {i+1}", top5)

    global_end = time.time()

    print("\n===== FIN DU CALCUL =====")
    print(f"Temps total (1 → 10) : {global_end - global_start:.3f} sec")
    print("==========================")

    top10 = ranks.takeOrdered(10, key=lambda x: -x[1])
    pretty("TOP 10 FINAL", top10)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit pagerank_rdd_ttl.py sample.ttl")
        sys.exit(1)
    main(sys.argv[1])
