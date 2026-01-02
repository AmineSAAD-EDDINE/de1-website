# Lab 2 – Design Note

## Objective
Dans ce lab, on a construit un mini data warehouse à partir des CSVs opérationnels.
On a utilisé des schémas explicites lors de l’ingestion et construit un schéma en étoile
avec des dimensions et une table de faits.

## Keys
- dim_user : `user_id` comme clé primaire
- dim_product : `product_id` comme clé primaire
- dim_brand : `brand` comme clé primaire
- dim_category : `category` comme clé primaire
- dim_date : `date_id` comme clé primaire
- fact_sales : clés étrangères vers toutes les dimensions (`user_id`, `product_id`, `brand`, `category`, `date_id`)

## Partitions
- Les tables de faits sont partitionnées sur `year` et `month` pour optimiser
les scans temporels.
- Les dimensions restent non partitionnées (taille raisonnable).

## Broadcast rationale
- On a broadcasté les petites dimensions (brand, category, date) lors des joins
avec fact_sales pour réduire le shuffle et améliorer les performances.
- Les grandes dimensions ou tables de faits n’ont pas été broadcastées pour éviter
la surcharge mémoire.

## Execution notes
- Ingestion avec `StructType` et type casts explicites
- Comptage des lignes après lecture pour logging
- Plans physiques Spark capturés ([plan_ingest.txt](../../../quartz/static/lab2-practice/plan_ingest.txt), [plan_fact_join.txt](../../../quartz/static/lab2-practice/plan_fact_join.txt))
- Métriques loggées dans [lab2_metrics_log.csv](../../../quartz/static/lab2-practice/lab2_metrics_log.csv) pour traçabilité et reproductibilité

