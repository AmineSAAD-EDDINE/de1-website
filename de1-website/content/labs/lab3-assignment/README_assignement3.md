# Assignment 3 – Data Science and Spark Algorithms

>**Students:** Maxence DELEHELLE, Amine SAAD-EDDINE   
>**Teacher:** Badr TAJINI     
>**Academic year:** 2025–2026  
>**Program:** Data & Applications - Engineering - (FD)   
>**Course:** Data Engineering I  
---

## Objective
Dans ce travail, on continue à exploiter le data warehouse construit dans l’Assignment 2.
On réalise une analyse des données avec SQL et PySpark DataFrames,
et on implémente des algorithmes RDD pour calculer des moyennes et effectuer des jointures.

## Scope
On a trois axes principaux :
1. Analyse de données avec SQL et DataFrame API pour répondre aux questions du notebook.
2. Implémentation de deux familles d’algorithmes RDD :
   - Moyennes (naïve et optimisée)
   - Joins (shuffle join et hash join)
3. Benchmark et comparaison des performances, en expliquant le choix des stratégies de jointure
   selon la taille des données et la distribution des clés.

## Method
- Préférence aux DataFrames pour l’analyse et RDDs pour les exercices demandés.
- Ingestion avec schémas explicites et conversion des timestamps si nécessaire.
- Exécution déterministe avec timezone fixe et seed pour la reproductibilité.
- Plans Spark et métriques loggés pour traçabilité et vérification.


