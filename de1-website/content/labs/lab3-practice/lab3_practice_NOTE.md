# Lab 3 – Row vs Column Queries and Spark Optimizations

>**Students:** Maxence DELEHELLE, Amine SAAD-EDDINE   
>**Teacher:** Badr TAJINI     
>**Academic year:** 2025–2026  
>**Program:** Data & Applications - Engineering - (FD)   
>**Course:** Data Engineering I  
---

## Objective
Dans ce lab, on a exploré l’ingestion de données avec un schéma explicite
et comparé l’exécution de requêtes en mode row et column.
On a également étudié l’impact du broadcast et du partitionnement sur les performances.

## Method
- Ingestion des CSV avec `StructType` et types explicites.
- Création de DataFrames row et column pour comparaison.
- Exécution des requêtes Q1 à Q3 et vérification que les résultats sont identiques.
- Application du broadcast pour certaines jointures afin de réduire le shuffle.
- Capture des plans physiques Spark et des métriques pour traçabilité.

## Trade-offs and Observations
- CSV : simple, lisible, mais lente lecture et absence de compression.
- Parquet : lecture plus rapide, compression intégrée, support des partitions.
- Partitionnement : améliore les scans sur colonnes filtrées, réduit I/O.
- Broadcast : efficace pour les petites tables, réduit shuffle, mais attention à la mémoire.

