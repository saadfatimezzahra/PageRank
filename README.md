# PageRank

# Implementation RDD

Cette version utilise les RDD de Spark, ce qui permet de contrôler directement la façon dont les données sont distribuées dans le cluster. Pour éviter les échanges inutiles entre les nœuds, la structure du graphe (links) est fixée avec partitionBy, ce qui réduit fortement le “shuffle”. Les rangs (ranks) utilisent le même partitionnement, donc les jointures se font localement et sont plus rapides. Les pages qui n’ont pas de liens sortants (dangling nodes) sont gérées séparément pour que leur score soit bien redistribué. Cette approche est plus bas niveau, mais elle permet d’optimiser manuellement l’algorithme comme expliqué dans l’article NSDI.

# Implementation DF

Cette version utilise l’API DataFrame de Spark, qui est plus haut niveau et profite automatiquement des optimisations du moteur Spark. Les données sont nettoyées avec des fonctions SQL, ce qui rend le traitement plus rapide. À chaque itération du PageRank, localCheckpoint() est utilisé pour réduire la taille du plan d’exécution et éviter les problèmes de mémoire. L’algorithme conserve toutes les pages du graphe grâce à un left_outer_join, même celles qui n’ont aucun lien sortant. Grâce à Catalyst, Spark optimise la plupart des opérations et rend cette version plus simple à écrire et souvent plus rapide à exécuter.