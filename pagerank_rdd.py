from pyspark.sql import SparkSession
from operator import add
import sys

DAMPING = 0.85
ITERATIONS = 10


def main(input_path, sample_ratio=1.0):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-CSV") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print(f"\n🔵 Lecture du fichier : {input_path}")
    print(f"🔵 Sample ratio (10% = 0.1) : {sample_ratio}\n")

    # 1️⃣ Lire edges.csv (src,dst)
    # Format attendu : <url1>,<url2>
    edges = (
        spark.read.csv(input_path, header=False)
             .rdd
             .map(lambda row: (row[0], row[1]))       # (src, dst)
             .filter(lambda x: x[0] is not None and x[1] is not None)
    )

    # 2️⃣ Appliquer un échantillonnage (pour 10% dans le cloud)
    if 0 < sample_ratio < 1:
        edges = edges.sample(False, sample_ratio, seed=42)

    edges = edges.distinct().cache()

    print(f"🔵 Nombre total d'edges : {edges.count()}")

    # 3️⃣ Construire la liste d'adjacence
    links = edges.groupByKey().mapValues(list).cache()

    # Partitionnement pour éviter les shuffles sur links
    num_partitions = links.getNumPartitions()
    links = links.partitionBy(num_partitions).cache()

    # 4️⃣ Pages uniques
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct()
    print(f"🔵 Nombre de pages uniques : {pages.count()}\n")

    # Initialisation des ranks = 1.0
    ranks = (
        pages.map(lambda p: (p, 1.0))
             .partitionBy(num_partitions)
             .cache()
    )

    print("\n===== 🚀 Début du calcul PageRank (RDD) =====\n")

    # 5️⃣ Boucle PageRank
    for i in range(ITERATIONS):
        print(f"🔁 Itération {i+1}/{ITERATIONS}")

        # join(links, ranks) ne provoque PAS de shuffle car partitionnement identique
        contribs = (
            links.join(ranks)
                 .flatMap(
                    lambda pr: [
                        (dst, pr[1][1] / len(pr[1][0]))
                        for dst in pr[1][0]
                    ]
                 )
        )

        # Somme des contributions (ce shuffle est inévitable)
        ranks = (
            contribs.reduceByKey(add)
                    .mapValues(lambda s: (1 - DAMPING) + DAMPING * s)
                    .partitionBy(num_partitions)
                    .cache()
        )

        # Affichage début + fin
        if i < 2 or i == ITERATIONS - 1:
            print("Top pages à cette itération :")
            for p, r in ranks.takeOrdered(5, key=lambda x: -x[1]):
                print(f"  {p:<40} {r}")
            print()

    # 6️⃣ Résultat final
    print("\n===== 🔥 TOP 20 RÉSULTATS FINAUX 🔥 =====")
    top20 = ranks.takeOrdered(20, key=lambda x: -x[1])
    for p, r in top20:
        print(f"{p:<50} {r}")

    print("\n🎉 FIN — PageRank RDD exécuté correctement !\n")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit pagerank_rdd.py <edges.csv> [sample_ratio]")
        sys.exit(1)

    input_path = sys.argv[1]
    sample_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    main(input_path, sample_ratio)
