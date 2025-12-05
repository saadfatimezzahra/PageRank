from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10


# ------------------------------------------------------------
# Fonction utilitaire pour affichage propre en tableau
# ------------------------------------------------------------
def pretty_print(title, rows, limit=10):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

    if not rows:
        print("(Aucune donnée)")
        print("=" * 80)
        return

    col1 = max(len(r[0]) for r in rows[:limit])
    col2 = 20

    print(f"{'src':<{col1}} | {'rank':<{col2}}")
    print("-" * (col1 + col2 + 3))

    for p, r in rows[:limit]:
        print(f"{p:<{col1}} | {r:<{col2}.6f}")

    print("=" * 80 + "\n")


# ------------------------------------------------------------
# Programme principal PageRank RDD
# ------------------------------------------------------------
def main(input_path, sample_ratio=1.0):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-CSV") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nLecture du fichier :", input_path)
    print("Sample ratio (10% = 0.1) :", sample_ratio, "\n")

    # Lecture edges.csv (src,dst)
    edges = (
        spark.read.csv(input_path, header=False)
             .rdd
             .map(lambda row: (row[0], row[1]))
             .filter(lambda x: x[0] is not None and x[1] is not None)
    )

    # Échantillonnage si demandé (pour Dataproc uniquement)
    if 0 < sample_ratio < 1:
        edges = edges.sample(False, sample_ratio, seed=42)

    edges = edges.distinct().cache()
    print("Nombre total d'edges :", edges.count())

    # Construction de la liste d'adjacence
    links = edges.groupByKey().mapValues(list).cache()

    # Partitionnement pour limiter les shuffles
    num_partitions = links.getNumPartitions()
    links = links.partitionBy(num_partitions).cache()

    # Pages uniques
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct()
    print("Nombre de pages uniques :", pages.count(), "\n")

    # Initialisation des ranks
    ranks = (
        pages.map(lambda p: (p, 1.0))
             .partitionBy(num_partitions)
             .cache()
    )

    print("\n===== Début du calcul PageRank (RDD) =====\n")

    # ------------------------------------------------------------
    # Timer global
    # ------------------------------------------------------------
    start_time = time.time()

    # ------------------------------------------------------------
    # Boucle PageRank
    # ------------------------------------------------------------
    for i in range(ITERATIONS):
        iter_start = time.time()

        print(f"Itération {i+1}/{ITERATIONS}")

        contribs = (
            links.join(ranks)
                 .flatMap(
                    lambda pr: [
                        (dst, pr[1][1] / len(pr[1][0]))
                        for dst in pr[1][0]
                    ]
                 )
        )

        ranks = (
            contribs.reduceByKey(add)
                    .mapValues(lambda s: (1 - DAMPING) + DAMPING * s)
                    .partitionBy(num_partitions)
                    .cache()
        )

        iter_end = time.time()
        print(f"Temps de l'itération {i+1} : {iter_end - iter_start:.3f} secondes")

        # Affichage du top 5 pour chaque itération
        top = ranks.takeOrdered(5, key=lambda x: -x[1])
        pretty_print(f"Top 5 pages à l'itération {i+1}", top, limit=5)

    # ------------------------------------------------------------
    # Timer global fin
    # ------------------------------------------------------------
    end_time = time.time()
    total_time = end_time - start_time

    print("=" * 80)
    print(f"Temps total d'exécution PageRank : {total_time:.3f} secondes")
    print("=" * 80)

    # Résultats finaux
    top20 = ranks.takeOrdered(20, key=lambda x: -x[1])
    pretty_print(
        "RÉSULTATS FINAUX (Top 20 pages triées par score descendant)",
        top20,
        limit=20
    )

    print("Fin — PageRank RDD exécuté correctement.\n")
    spark.stop()


# ------------------------------------------------------------
# Point d'entrée
# ------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit pagerank_rdd.py <edges.csv> [sample_ratio]")
        sys.exit(1)

    input_path = sys.argv[1]
    sample_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    main(input_path, sample_ratio)
