import sys
from pyspark.sql import SparkSession, functions as F

# ----------------------------
# Hyperparamètres du PageRank
# ----------------------------
DAMPING = 0.85       # facteur d'amortissement (Google = 0.85)
ITERATIONS = 10      # nombre d'itérations du PageRank


# =====================================================================
# UDF : Fonction pour parser une ligne TTL et retourner (src, dst)
# =====================================================================
@F.udf("struct<src:string, dst:string>")
def parse_ttl(line):
    """
    Parse une ligne du dataset DBpedia Wikilinks en format TTL.
    Retourne une structure (src, dst) si la ligne contient un lien Wiki.
    Sinon retourne None.

    Exemple de ligne :
    <http://dbpedia.org/resource/Paris> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/resource/France> .
    """
    if line is None:
        return None

    line = line.strip()

    # Ignorer les lignes vides, commentaires, préfixes
    if not line or line.startswith("@") or line.startswith("#"):
        return None

    # Nous ne gardons que les liens Wiki (wikiPageWikiLink)
    if "wikiPageWikiLink" not in line:
        return None

    parts = line.split()
    if len(parts) < 3:
        return None

    try:
        src_uri = parts[0].strip("<>")
        dst_uri = parts[2].strip("<>")

        # Vérifier que ce sont bien des URLs DBpedia
        if "dbpedia.org/resource/" not in src_uri or "dbpedia.org/resource/" not in dst_uri:
            return None

        # Extraire seulement le nom de la ressource : Paris, France, etc.
        src = src_uri.split("/")[-1]
        dst = dst_uri.split("/")[-1]

        if not src or not dst:
            return None

        return (src, dst)

    except:
        return None