from pyspark.sql import SparkSession
from operator import add
import sys
import time

# ==========================================
# CONFIGURATION
# ==========================================
DAMPING = 0.85
ITERATIONS = 10
NUM_PARTITIONS = 4  # Ajuster selon tes cœurs (2-4 pour ta machine)

def main(input_path):
    spark = SparkSession.builder \
        .appName("PageRank-Corrected-NSDI") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(f"--- Lecture de : {input_path} ---")

    # 1. Parsing Robuste
    def parse_ttl(line):
        # Format attendu: <src> <pred> <dst> .
        parts = line.split(" ")
        if len(parts) < 3: return None
        src = parts[0].strip('<>') 
        dst = parts[2].strip('<>')
        return (src, dst)

    raw_data = sc.textFile(input_path, minPartitions=NUM_PARTITIONS)
    edges = raw_data.map(parse_ttl).filter(lambda x: x is not None)

    # 2. Construction de l'Univers Complet (LA CORRECTION EST ICI)
    # On doit connaître TOUTES les pages, même celles qui n'apparaissent qu'en destination
    # Sinon, les pages populaires (qui reçoivent des liens) disparaissent de la structure.
    
    # Liste d'adjacence brute (uniquement les sources)
    out_links = edges.distinct().groupByKey()
    
    # Ensemble de TOUTES les pages (sources + destinations)
    all_pages = edges.flatMap(lambda x: x).distinct()
    
    # Construction du graphe complet : (Page, [Liste de voisins])
    # Si une page n'a pas de voisins, on met une liste vide [] (HashPartitioner ici !)
    links = all_pages.map(lambda p: (p, 0)) \
        .leftOuterJoin(out_links) \
        .mapValues(lambda x: x[1] if x[1] is not None else []) \
        .partitionBy(NUM_PARTITIONS) \
        .cache()

    # On force le calcul pour compter N et mettre en cache
    N = links.count()
    print(f"Nombre total de pages (N) : {N}")

    # 3. Initialisation des Ranks
    # On met 1.0 partout (ou 1/N, peu importe, ça converge vers la même chose)
    ranks = links.mapValues(lambda _: 1.0)

    print("\n" + "="*60)
    print(f"{'DÉBUT DU CALCUL':^60}")
    print("="*60)

    global_start = time.time()

    for i in range(ITERATIONS):
        iter_start = time.time()

        # A. Calcul de la masse "Dangling" (Masse des culs-de-sac)
        # Les noeuds qui ont une liste vide [] ne distribuent rien via le flatMap.
        # Leur rank est "perdu". On doit le calculer pour le redistribuer.
        # Astuce : On calcule la somme totale actuelle. Ce qui manque par rapport à N est la masse perdue.
        current_mass = ranks.values().sum()
        dangling_mass = N - current_mass

        # B. Distribution des contributions (Join Local car co-partitionné)
        contribs = links.join(ranks).flatMap(
            lambda x: compute_contribs(x[1][0], x[1][1])
        )

        # C. Somme des contributions reçues
        contribs_sum = contribs.reduceByKey(add)

        # D. Mise à jour des Ranks (Join complet sur 'links')
        # links contient TOUT LE MONDE. 
        # leftOuterJoin garantit que ceux qui n'ont reçu aucun vote (contribs_sum = None) restent en vie.
        ranks = links.leftOuterJoin(contribs_sum).mapValues(
            lambda x: update_rank(x[1], dangling_mass, N)
        )
        
        # Action : on récupère le top pour forcer le calcul et afficher
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
    """ Envoie le rank divisé par le nombre de voisins. """
    # Si urls est vide (empty iterable), la boucle for ne s'exécute pas.
    # Le rank n'est envoyé nulle part (il devient de la dangling mass).
    num_urls = len(urls)
    if num_urls > 0:
        val = rank / num_urls
        for url in urls:
            yield (url, val)

def update_rank(sum_contribs, dangling_mass, N):
    """ (1-d) + d * (SommeIn + Dangling/N) """
    # note: (1-d) est souvent implémenté comme (1-d)/N ou juste (1-d) selon les papiers.
    # Ici on utilise la version standard de Spark PageRank: 0.15 + 0.85 * ...
    received = sum_contribs if sum_contribs is not None else 0.0
    return 0.15 + 0.85 * (received + dangling_mass / N)


# ================= AFFICHAGE =================

def print_iter(i, dt, rows):
    print(f"\n> Iter {i} ({dt:.2f}s)")
    print("-" * 65)
    for idx, (url, r) in enumerate(rows, 1):
        # url courte
        u = (url[:45] + '..') if len(url) > 45 else url
        print(f"  {idx}. {u:<48} : {r:.4f}")

def print_final(rows):
    print("\n" + "="*65)
    print(f"{'RESULTATS FINAUX':^65}")
    print("="*65)
    print(f"| {'#':<3} | {'Page URL':<45} | {'Score':>10} |")
    print("-" * 65)
    for idx, (url, r) in enumerate(rows, 1):
        u = (url[:42] + '...') if len(url) > 45 else url
        print(f"| {idx:<3} | {u:<45} | {r:10.4f} |")
    print("="*65)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pagerank_fixed.py data.ttl")
    else:
        main(sys.argv[1])