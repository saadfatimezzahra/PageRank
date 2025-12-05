from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10
WIKILINK_PREDICATE = "<http://dbpedia.org/ontology/wikiPageWikiLink>"


def read_ttl(sc, input_path, sample_ratio):
    """Lit un fichier TTL ou TTL.BZ2 et retourne des edges (src, dst)."""

    rdd = sc.textFile(input_path)

    # Nettoyage des lignes
    rdd = rdd.map(lambda l: l.strip()) \
             .filter(lambda l: len(l) > 0 and not l.startswith("#"))

    def parse_line(line):
        parts = line.split(" ")
        if len(parts) < 4:
            return None

        src = parts[0]
        pred = parts[1]
        dst = parts[2]

        if pred != WIKILINK_PREDICATE:
            return None

        return (src[1:-1], dst[1:-1])  # enlever < >

    # Filtrer uniquement edges valides
    rdd = rdd.map(parse_line).filter(lambda x: x is not None)

    # Échantillonnage demandé par le prof (ex: 0.1 = 10%)
    if 0 < sample_ratio < 1:
        rdd = rdd.sample(False, sample_ratio, seed=42)

    return rdd.distinct().cache()


def pretty_print(title, rows, limit=10):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    if not rows:
        print("(vide)")
        print("=" * 80)
        return

    col1 = max(len(r[0]) for r in rows[:limit])
    col2 = 20

    print(f"{'Page':<{col1}} | Rank")
    print("-" * (col1 + col2 + 3))

    for p, r in rows[:limit]:
        print(f"{p:<{col1}} | {r}")

    print("=" * 80 + "\n")


def main(input_path, sample_ratio=1.0):

    spark = SparkSession.builder \
        .appName("PageRank-RDD-TTL") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nLecture du fichier :", input_path)
    print("Sample ratio :", sample_ratio)

    edges = read_ttl(sc, input_path, sample_ratio)
    print("Nombre d'edges :", edges.count())

    # Adjacency list
    links = edges.groupByKey().mapValues(list).cache()

    # Pages uniques
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()
    num_pages = pages.count()
    print("Nombre de pages uniques :", num_pages)

    # Initialisation des ranks
    ranks = pages.map(lambda p: (p, 1.0)).cache()

    # Teleportation uniforme
    base = (1 - DAMPING) / num_pages

    # Eviter les shuffles : même partitionnement
    num_partitions = links.getNumPartitions()
    links = links.partitionBy(num_partitions).cache()
    ranks = ranks.partitionBy(num_partitions).cache()

    print("\n===== Début du PageRank RDD =====\n")

    for i in range(ITERATIONS):
        print(f"Itération {i+1}/{ITERATIONS}")
        iter_start = time.time()

        # JOIN : aucune redistribution => évite les shuffles
        joined = links.join(ranks)

        # Contributions sortantes
        contribs = joined.flatMap(
            lambda x: [
                (dst, x[1][1] / len(x[1][0]))
                for dst in x[1][0]
            ]
        )

        # Détection des dangling nodes (pages sans liens sortants)
        out_degrees = links.mapValues(len)
        dangling_rank = (
            ranks.join(out_degrees)
                 .filter(lambda x: x[1][1] == 0)
                 .map(lambda x: x[1][0])
                 .sum()
        )

        # Mise à jour du PageRank
        ranks = (
            contribs.reduceByKey(add)
                    .mapValues(lambda s: base + DAMPING * (s + dangling_rank / num_pages))
                    .partitionBy(num_partitions)
                    .cache()
        )

        iter_end = time.time()
        print(f"Temps iteration {i+1} : {iter_end - iter_start:.3f}s")

        top5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        pretty_print(f"Top 5 après itération {i+1}", top5)

    # Résultats finaux
    top20 = ranks.takeOrdered(20, key=lambda x: -x[1])
    pretty_print("TOP 20 FINAL", top20)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit pagerank_rdd_ttl.py <file.ttl(.bz2)> [sample_ratio]")
        sys.exit(1)

    input_path = sys.argv[1]
    sample_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    main(input_path, sample_ratio)
