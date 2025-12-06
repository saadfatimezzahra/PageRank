# PageRank

# Implementation RDD

Cette version utilise les RDD de Spark, ce qui permet de contrôler directement la façon dont les données sont distribuées dans le cluster. Pour éviter les échanges inutiles entre les nœuds, la structure du graphe (links) est fixée avec partitionBy, ce qui réduit fortement le “shuffle”. Les rangs (ranks) utilisent le même partitionnement, donc les jointures se font localement et sont plus rapides. Les pages qui n’ont pas de liens sortants (dangling nodes) sont gérées séparément pour que leur score soit bien redistribué. Cette approche est plus bas niveau, mais elle permet d’optimiser manuellement l’algorithme comme expliqué dans l’article NSDI.

# Implementation DF

Cette version utilise l’API DataFrame de Spark, qui est plus haut niveau et profite automatiquement des optimisations du moteur Spark. Les données sont nettoyées avec des fonctions SQL, ce qui rend le traitement plus rapide. À chaque itération du PageRank, localCheckpoint() est utilisé pour réduire la taille du plan d’exécution et éviter les problèmes de mémoire. L’algorithme conserve toutes les pages du graphe grâce à un left_outer_join, même celles qui n’ont aucun lien sortant. Grâce à Catalyst, Spark optimise la plupart des opérations et rend cette version plus simple à écrire et souvent plus rapide à exécuter.

Resultat DF (2Noeuds):

Temps Total : 210.26s

gcloud storage cat gs://$BUCKET/output/part-00000-4ff317f2-0ed5-4780-8d80-47c001d88bbd-c000.csv
id,rank
http://dbpedia.org/resource/Category:Background_asteroids,820.2171008874903
http://dbpedia.org/resource/Category:Named_minor_planets,776.5598654343554
http://dbpedia.org/resource/List_of_years_in_science,288.76342492834783
http://dbpedia.org/resource/Table_of_years_in_literature,288.76342492834783
http://dbpedia.org/resource/1000_(number),268.35265207476596
http://dbpedia.org/resource/Category:Discoveries_by_SCAP,201.98237486335663
http://dbpedia.org/resource/List_of_minor_planets:_10001–11000,188.5668714559716
http://dbpedia.org/resource/Category:Astronomical_objects_discovered_in_1998,173.89589710326348
http://dbpedia.org/resource/American_football,154.34466091426665
http://dbpedia.org/resource/Habeas_corpus_petitions_of_Guantanamo_Bay_detainees,148.325433610981


Resultats RDD (2noeuds):

Temps TOTAL PageRank : 349.719 sec


========================================================================================================================
                                                RESULTATS FINAUX TOP 10                                                 
========================================================================================================================
| #   | Page URL                                                                                   |         Rank |
------------------------------------------------------------------------------------------------------------------------
| 1   | http://dbpedia.org/resource/Category:Background_asteroids                                  |   659.399772 |
| 2   | http://dbpedia.org/resource/Category:Named_minor_planets                                   |   623.441737 |
| 3   | http://dbpedia.org/resource/List_of_years_in_science                                       |   232.507412 |
| 4   | http://dbpedia.org/resource/Table_of_years_in_literature                                   |   232.507412 |
| 5   | http://dbpedia.org/resource/1000_(number)                                                  |   215.491269 |
| 6   | http://dbpedia.org/resource/Category:Discoveries_by_SCAP                                   |   162.624421 |
| 7   | http://dbpedia.org/resource/List_of_minor_planets:_10001–11000                             |   151.815228 |
| 8   | http://dbpedia.org/resource/Category:Astronomical_objects_discovered_in_1998               |   139.979088 |
| 9   | http://dbpedia.org/resource/Habeas_corpus_petitions_of_Guantanamo_Bay_detainees            |   119.429123 |
| 10  | http://dbpedia.org/resource/Category:Discoveries_by_LINEAR                                 |   107.232030 |
===================================================================================================================
