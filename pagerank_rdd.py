from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10


# ------------------------------------------------------------
# Lecture d'un fichier TTL extrait avec "head -n"
# Format d'une ligne : <src> <predicate> <dst> .
# On garde TOUTES les relations (tu veux tester sur ton fichier réduit)
# ------------------------------------------------------------
def read_ttl(sc, input_path):
    rdd = sc.textFile(input_path)

    # Enlever lignes vides
    rdd = rdd.map(lambda l: l.strip()).filter(lambda l: len(l) > 0)

    def parse_line(line):
        parts = line.split(" ")
        if len(parts) < 3:
            return None
        src = parts[0][1:-1]         # enlever <>
        dst = parts[2][1:-1]         # enlever <>
        return (src, dst)

    return rdd.map(parse_line).filter(lambda x: x is not None).cache()


# ------------------------------------------------------------
# Affichage propre des résultats
# ------------------------------------------------------------
def pretty_print(title, rows, limit=10):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    if not rows:
        print("(vide)")
        print("=" * 70)
        return

    col1 = max(len(r[0]) for r in rows[:limit])
    print(f"{'Page':<{col1}} | Rank")
    print("-" * (col1 + 20))

    for p, r in rows[:limit]:
        print(f"{p:<{col1}} | {r:.6f}")

    print("=" * 70 + "\n")


# ------------------------------------------------------------
# Algorithme PageRank RDD
# ------------------------------------------------------------
def main(input_path):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-SIMPLE") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nLecture du fichier :", input_path)

    # 1) Lire edges
    edges = read_ttl(sc, input_path)
    print("Nombre total d'edges :", edges.count())

    # 2) Ensemble des pages
    pages = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()
    num_pages = pages.count()
    print("Nombre total de pages :", num_pages)

    # 3) Liste d'adjacence (src → liste de dst)
    raw_links = edges.groupByKey().mapValues(list)

    # IMPORTANT :
    # S'assurer que chaque page a AU MOINS une liste vide
    links = (
        pages.map(lambda p: (p, []))
             .leftOuterJoin(raw_links)
             .mapValues(lambda x: x[1] if x[1] is not None else x[0])
             .cache()
    )

    # 4) Ranks initiaux
    ranks = pages.map(lambda p: (p, 1.0)).cache()

    base = (1.0 - DAMPING) / num_pages

    print("\n===== DÉBUT DU CALCUL PAGERANK =====\n")

    global_start = time.time()

    # ------------------------------------------------------------
    # Boucle des 10 itérations
    # ------------------------------------------------------------
    for i in range(ITERATIONS):
        iter_start = time.time()
        print(f"Itération {i+1}/{ITERATIONS}")

        # Degré sortant par page
        out_deg = links.mapValues(len)

        # Rank des pages sans liens sortants
        dangling = (
            ranks.join(out_deg)
                 .filter(lambda x: x[1][1] == 0)
                 .map(lambda x: x[1][0])
                 .sum()
        )

        # Contributions des pages ayant des voisins
        contribs = (
            links.join(ranks)
                 .flatMap(lambda x: [
                     (dst, x[1][1] / len(x[1][0]))
                     for dst in x[1][0] if len(x[1][0]) > 0
                 ])
        )

        # Mise à jour des ranks
        ranks = (
            contribs.reduceByKey(add)
                    .mapValues(lambda s: base + DAMPING * (s + dangling / num_pages))
                    .cache()
        )

        iter_end = time.time()
        print(f"Temps de l'itération {i+1} : {iter_end - iter_start:.3f} sec")

        # Afficher top 5 de cette itération
        top5 = ranks.takeOrdered(5, key=lambda x: -x[1])
        pretty_print(f"Top 5 après itération {i+1}", top5)

    global_end = time.time()

    print("=" * 70)
    print(f"Temps TOTAL PageRank (10 itérations) : {global_end - global_start:.3f} sec")
    print("=" * 70)

    # ------------------------------------------------------------
    # TOP 10 FINAL
    # ------------------------------------------------------------
    top10 = ranks.takeOrdered(10, key=lambda x: -x[1])
    pretty_print("TOP 10 FINAL", top10, limit=10)

    spark.stop()


# ------------------------------------------------------------
# Entrée programme
# ------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage : spark-submit pagerank_rdd_ttl.py sample_wikilinks.ttl")
        sys.exit(1)

    input_path = sys.argv[1]
    main(input_path)
