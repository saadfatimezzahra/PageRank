from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10
WIKILINK_PREDICATE = "<http://dbpedia.org/ontology/wikiPageWikiLink>"


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
# Lecture du fichier TTL (RDF triples)
# ------------------------------------------------------------
def read_ttl(sc, input_path, sample_ratio):
    # Spark décompresse automatiquement les .bz2
    rdd = sc.textFile(input_path)

    # Nettoyage des lignes vides / commentaires
    rdd = (
        rdd.map(lambda line: line.strip())
           .filter(lambda line: len(line) > 0 and not line.startswith("#"))
    )

    # Format DBpedia :
    # <src> <predicate> <dst> .
    def parse_line(line):
        parts = line.split(" ")
        # On attend au moins : src, pred, dst, "."
        if len(parts) < 4:
            return None

        src = parts[0]
        pred = parts[1]
        dst = parts[2]

        # Garder uniquement les liens wikiPageWikiLink
        if pred != WIKILINK_PREDICATE:
            return None

        # enlever < >
        src = src[1:-1]
        dst = dst[1:-1]

        return (src, dst)

    rdd = rdd.map(parse_line).filter(lambda x: x is not None)

    # Échantillonnage (10% du dataset par ex.)
    if 0 < sample_ratio < 1:
        rdd = rdd.sample(False, sample_ratio, seed=42)

    return rdd.distinct().cache()


# ------------------------------------------------------------
# Programme principal PageRank RDD compatible TTL
# ------------------------------------------------------------
def main(input_path, sample_ratio=1.0):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-TTL") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nLecture du fichier :", input_path)
    print("Sample ratio (10% = 0.1) :", sample_ratio, "\n")

    # 1) Lire TTL
    edges = read_ttl(sc, input_path, sample_ratio)

    num_edges = edges.count()
    print("Nombre total d'edges :", num_edges)

    # 2) Liste d'adjacence
    links = edges.groupByKey().mapValues(list).cache()

    # 3) Partitionnement
    num_partitions = links.getNumPartitions()
    links = links.partitionBy(num_partitions).cache()

    # 4) Pages uniques
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()
    num_pages = pages.count()
    print("Nombre de pages uniques :", num_pages, "\n")

    # 5) Initialisation des ranks
    ranks = (
        pages.map(lambda p: (p, 1.0))
             .partitionBy(num_partitions)
             .cache()
    )

    # terme de base de PageRank (téléportation uniforme)
    base = (1.0 - DAMPING) / num_pages

    print("\n===== Début du calcul PageRank (RDD) =====\n")

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
                    .mapValues(lambda s: base + DAMPING * s)
                    .partitionBy(num_partitions)
                    .cache()
        )

        iter_end = time.time()
        print(f"Temps de l'itération {i+1} : {iter_end - iter_start:.3f} secondes")

        top = ranks.takeOrdered(5, key=lambda x: -x[1])
        pretty_print(f"Top 5 pages à l'itération {i+1}", top, limit=5)

    end_time = time.time()

    print("=" * 80)
    print(f"Temps total d'exécution PageRank : {end_time - start_time:.3f} secondes")
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
        print("Usage: spark-submit pagerank_rdd_ttl.py <file.ttl or file.ttl.bz2> [sample_ratio]")
        sys.exit(1)

    input_path = sys.argv[1]
    sample_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    main(input_path, sample_ratio)
