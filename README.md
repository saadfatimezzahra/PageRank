# PageRank Tests sur Wikipedia

---

## 1. Introduction
Ce document présente les résultats de notre étude sur l’algorithme PageRank appliqué au dataset DBpedia WikiLinks. Deux implémentations ont été développées : une version RDD et une version DataFrame.
L’objectif est d’évaluer leurs performances et leur comportement sur plusieurs configurations de clusters Dataproc.

---

## 2. Configurations Testées
### 2.1 n=2
| Master        | Nœuds       | Total vCPU | RAM Totale | Type Machine  |
|---------------|------------|------------|------------|---------------|
| 1×4 vCPU      | 2×4 vCPU   | 12 vCPU    | 52 GB      | e2-standard-4 |

### 2.2 n=4
| Master        | Nœuds       | Total vCPU | RAM Totale | Type Machine  |
|---------------|------------|------------|------------|---------------|
| 1×4 vCPU      | 4×4 vCPU   | 20 vCPU    | 84 GB      | e2-standard-4 |

### 2.3 n=6
| Master        | Nœuds       | Total vCPU | RAM Totale | Type Machine  |
|---------------|------------|------------|------------|---------------|
| 1×4 vCPU      | 6×4 vCPU   | 28 vCPU    | 118 GB     | e2-standard-4 |

**Paramètres Algorithme**
- Itérations: 10
- Damping factor: 0.85

---

## 3. Temps d'Exécution et Graphiques
### 3.1 n=2
| Implémentation | Temps (s) | Temps (min) |
|----------------|------------|-------------|
| RDD            | 4483       | 75          |
| DataFrame      | 2001       | 33          |

**Graphique Performance (minutes)**
```
80 | RDD ■
70 |
60 |
50 |
40 |
30 | DataFrame □
20 |
10 |
 0 |------
     n=2
```

### 3.2 n=4
| Implémentation | Temps (s) | Temps (min) |
|----------------|------------|-------------|
| RDD            | 2335       | 39          |
| DataFrame      | 1280       | 22          |

**Graphique Performance (minutes)**
```
40 | RDD ■
35 |
30 |
25 |
20 | DataFrame □
15 |
10 |
 0 |------
     n=4v
```

### 3.3 n=6
| Implémentation | Temps (s) | Temps (min) |
|----------------|------------|-------------|
| RDD            | 1668       | 28          |
| DataFrame      | 900        | 15          |

**Graphique Performance (minutes)**
```
30 | RDD ■
25 |
20 |
15 | DataFrame □
10 |
 0 |------
     n=6
```

---

## 4. Scalabilité
### 4.1 RDD
#### n=2 → n=4
| Speedup | Efficacité |
|---------|------------|
| 1.92x   | 96%        |

#### n=4 → n=6
| Speedup | Efficacité |
|---------|------------|
| 1.40x   | 93%        |

#### n=2 → n=6
| Speedup | Efficacité |
|---------|------------|
| 2.69x   | 90%        |

### 4.2 DataFrame
#### n=2 → n=4
| Speedup | Efficacité |
|---------|------------|
| 1.56x   | 78%        |

#### n=4 → n=6
| Speedup | Efficacité |
|---------|------------|
| 1.42x   | 95%        |

#### n=2 → n=6
| Speedup | Efficacité |
|---------|------------|
| 2.23x   | 74%        |

---

## 5. Validation des Résultats et Problèmes
### 5.1 n=2
| Vérification              | Statut |
|----------------------------|--------|
| Job terminé                | ✅     |
| Centre Wikipedia           | Category:Living people ✅ |
| Cohérence scores           | OK     |

**Problèmes**
| Type                        | Commentaire |
|------------------------------|-------------|
| Sauvegarde RDD               | Fichier existant, non bloquant |
| Warnings YARN                | Perte de replicas, job continué |

### 5.2 n=4
| Vérification              | Statut |
|----------------------------|--------|
| Job terminé                | ✅     |
| Centre Wikipedia           | Category:Living people ✅ |
| Cohérence scores           | OK     |

**Problèmes**
| Type                        | Commentaire |
|------------------------------|-------------|
| Overhead communication       | Légèrement plus de temps pour RDD |

### 5.3 n=6
| Vérification              | Statut |
|----------------------------|--------|
| Job terminé                | ✅     |
| Centre Wikipedia           | Category:Living people ✅ |
| Cohérence scores           | OK     |

**Problèmes**
| Type                        | Commentaire |
|------------------------------|-------------|
| Overhead communication       | Gains supplémentaires limités |

---

## 6. Conclusions par Nœuds
| n  | Implémentation | Conclusion |
|----|----------------|------------|
| 2  | RDD            | Correct mais plus lent |
|    | DataFrame      | Efficace et stable |
| 4  | RDD            | Scalabilité presque linéaire, correct |
|    | DataFrame      | Très rapide et stable |
| 6  | RDD            | Bonnes performances mais rendements décroissants |
|    | DataFrame      | Meilleure performance, stable |

---

## 7. Fichiers de Résultats et Arborescence
```
results/
    2-nodes/
        rdd_results.csv
        df_results.csv
    4-nodes/
        rdd_results.csv
        df_results.csv
    6-nodes/
        rdd_results.csv
        df_results.csv
```

---

## 8. Configuration Technique et Scripts
### Cluster Dataproc
```bash
gcloud dataproc clusters create pagerank-cluster-nX \
    --region=europe-west1 \
    --zone=europe-west1-b \
    --master-machine-type=e2-standard-4 \
    --master-boot-disk-size=100 \
    --num-workers=X \
    --worker-machine-type=e2-standard-4 \
    --worker-boot-disk-size=100 \
    --image-version=2.1-debian11 \
    --max-idle=10m \
    --properties="spark:spark.executor.memory=14g,spark:spark.driver.memory=15g,spark:spark.executor.cores=3,spark:spark.sql.shuffle.partitions=200"
```

### Scripts Python
- `pagerank_rdd.py` : RDD Spark, transformations manuelles, bas niveau
- `pagerank_df.py` : DataFrame Spark, optimisations Catalyst, AQE, Tungsten engine

---

**Équipe:** OUEDRHIRI Mohammed / SAAD Fatimezzahra  


