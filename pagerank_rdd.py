from pyspark.sql import SparkSession
from operator import add
import sys
import time

DAMPING = 0.85
ITERATIONS = 10

def main(input_path):
    spark = SparkSession.builder \
        .appName("PageRank-RDD-NSDI-Optimized") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Définir un nombre de partitions (ex: 4 ou 8 pour ton petit cluster)
    # Important pour le parallélisme et éviter les fichiers trop petits
    NUM_PARTITIONS = 4 

    # 1) Lecture et Parsing
    raw_lines = sc.textFile(input_path, minPartitions=NUM_PARTITIONS)
    
    def parse_line(line):
        parts = line.split(" ")
        if len(parts) < 3: return None
        return (parts[0][1:-1], parts[2][1:-1]) # src, dst

    # On ne garde que les liens valides
    edges = raw_lines.map(parse_line).filter(lambda x: x is not None)

    # 2) Construction du Graphe (Statique)
    # C'est ICI que l'optimisation NSDI se joue : partitionBy
    # On groupe par URL source.
    links = edges.distinct().groupByKey().partitionBy(NUM_PARTITIONS).cache()
    
    # On force le calcul pour mettre en cache maintenant
    print(f"Graph count: {links.count()}")

    # 3) Initialisation des Ranks
    # Astuce: utiliser mapValues sur 'links' préserve le partitionneur !
    # ranks et links sont maintenant co-partitionnés.
    ranks = links.mapValues(lambda _: 1.0)

    # Note: Dans ton code original, tu gérais les pages sans liens sortants (dangling) 
    # via un join complexe. Pour simplifier et accélérer, on se concentre sur le coeur
    # de l'algo. Si une page n'a pas de lien sortant, elle est juste une "sink".
    # (Pour une implémentation stricte, on calcule la masse perdue et on la redistribue).

    print("\n===== DÉBUT DU CALCUL PAGERANK OPTIMISÉ =====\n")
    global_start = time.time()

    for i in range(ITERATIONS):
        iter_start = time.time()

        # 4) Le Join Optimisé
        # Comme links et ranks ont le même partitionneur, Spark fait un "Local Join" 
        # (pas de shuffle des links, juste échange des ranks si nécessaire).
        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )

        # 5) Mise à jour des ranks
        # reduceByKey va utiliser le partitionneur existant si possible
        # mapValues préserve le partitionnement pour le tour suivant
        ranks = contribs.reduceByKey(add, numPartitions=NUM_PARTITIONS) \
            .mapValues(lambda rank: 0.15 + 0.85 * rank)
        
        # Action pour forcer le calcul et mesurer le temps
        # count() est bon marché, ou take(1)
        ranks.count() 
        
        print(f"Itération {i+1}: {time.time() - iter_start:.3f} sec")

    print(f"Temps TOTAL : {time.time() - global_start:.3f} sec")
    
    # Affichage résultat
    top10 = ranks.takeOrdered(10, key=lambda x: -x[1])
    for (link, rank) in top10:
        print(f"{link} : {rank}")

    spark.stop()

def compute_contribs(urls, rank):
    """Calcule les contributions envoyées aux voisins."""
    num_urls = len(urls)
    if num_urls > 0:
        contrib = rank / num_urls
        for url in urls:
            yield (url, contrib)
    else:
        # Cas "Dangling node" (si présent dans la liste)
        yield ("__DANGLING__", rank) # Astuce pour capturer la masse perdue si besoin