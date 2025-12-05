#PageRank RDD Implementation

import sys
from operator import add
from pyspark.sql import SparkSession

DAMPING = 0.85
ITERATIONS = 10

def main(input_path: str, sample_ratio: float):
    """
    PageRank version RDD basé sur un fichier edges.csv :
    chaque ligne = src,dst (déjà nettoyé).
    sample_ratio = 0.1  -> on garde 10% des edges.
    """

    spark = SparkSession.builder.appName("PageRank-RDD").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print(f"\n🔵 Lecture de : {input_path}")
    print(f"🔵 Sample ratio = {sample_ratio}\n")

    # 1. Lire le CSV en RDD (sans header)
    #    Chaque élément = Row('_c0', '_c1')
    raw_rdd = (
        spark.read.csv(input_path, header=False)
             .rdd
             .map(lambda row: (row[0], row[1]))  # (src, dst)
    )

    # 10% des données (ou autre fraction passée en paramètre)
    if 0 < sample_ratio < 1.0:
        raw_rdd = raw_rdd.sample(False, sample_ratio, seed=42)

    # On enlève les doublons éventuels
    edges = raw_rdd.distinct()          # (src, dst)

    # 2. Construire la liste d'adjacence : (src, [dst1, dst2, ...])
    #    C'est ici qu'on va être attentifs au partitionnement.
    links = edges.groupByKey().mapValues(list)

    # Nombre de partitions (à adapter si besoin)
    num_partitions = links.getNumPartitions()  # on garde celle de Spark

    # On "fixe" le partitionnement une bonne fois pour toutes
    links = links.partitionBy(num_partitions).cache()

    # 3. Initialiser les ranks à 1.0 pour chaque page
    pages = (
        edges.flatMap(lambda e: [e[0], e[1]])
             .distinct()
    )

    ranks = pages.map(lambda p: (p, 1.0)) \
                 .partitionBy(num_partitions) \
                 .cache()

    print(f"🔵 Nombre de pages uniques : {pages.count()}")
    print(f"🔵 Nombre de partitions    : {num_partitions}\n")

    # 4. Boucle PageRank
    for i in range(ITERATIONS):
        print(f"🔁 Itération {i+1}/{ITERATIONS}")

        # join(links, ranks) NE reshuffle PAS links,
        # car links & ranks ont le même partitionnement.
        # -> on évite de reshuffler les "neighbours" (liste d'adjacence)
        contribs = (
            links.join(ranks)          # (page, ([neighbours], rank))
                 .flatMap(
                     lambda pr: [
                         (dst, pr[1][1] / len(pr[1][0]))
                         for dst in pr[1][0]
                     ]
                 )                    # (dst, contribution)
        )

        # reduceByKey implique un shuffle sur les contributions
        # (on ne peut pas l'éviter complètement pour PageRank),
        # mais on ne reshuffle plus la structure links.
        ranks = (
            contribs
            .reduceByKey(add)
            .mapValues(lambda s: (1 - DAMPING) + DAMPING * s)
            .partitionBy(num_partitions)
            .cache()
        )

        if i < 2 or i == ITERATIONS - 1:
            print("Top 5 pages à cette itération :")
            for page, r in ranks.takeOrdered(5, key=lambda x: -x[1]):
                print(f"  {page:<40} {r}")
            print()

    # 5. Résultat final
    print("\n===== 🔥 TOP 20 PageRank (RDD) 🔥 =====")
    top20 = ranks.takeOrdered(20, key=lambda x: -x[1])
    for page, r in top20:
        print(f"{page:<40} {r}")

    print("\n🎉 Calcul RDD terminé.\n")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage : spark-submit pagerank_rdd.py <input_path> [sample_ratio]")
        sys.exit(1)

    input_path = sys.argv[1]
    sample_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1  # 10% par défaut

    main(input_path, sample_ratio)
