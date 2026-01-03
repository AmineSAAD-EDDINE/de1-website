# Assignment 2 – Spark ETL & Data Warehouse

>**Students:** Maxence DELEHELLE, Amine SAAD-EDDINE   
>**Teacher:** Badr TAJINI     
>**Academic year:** 2025–2026  
>**Program:** Data & Applications - Engineering - (FD)   
>**Course:** Data Engineering I  
---

## Objective
L’objectif de ce travail est d’explorer un schéma opérationnel e-commerce
et de construire un mini data warehouse à l’aide de Spark.
On met en œuvre un pipeline ETL reproductible basé sur des bonnes pratiques Spark.

## Operational Schema
Le dump opérationnel contient les tables suivantes :
- user (user_id, gender, birthdate)
- session (session_id, user_id)
- product (product_id, brand, category, product_name)
- product_name (category, product_name, description)
- events (event_time, event_type, session_id, product_id, price)
- category (category, description)
- brand (brand, description)

On identifie les clés primaires et étrangères afin de reconstruire les sessions
et d’analyser les achats (qui a acheté quoi, quand, et à quel prix).

## Method
On suit un pipeline ETL classique avec Spark :
- extraction des tables opérationnelles vers des fichiers CSV
- chargement avec schémas explicites et conversions de types
- parsing des timestamps en UTC et déduplication
- construction des dimensions (user, brand, category, product, date)
- construction d’une table de faits basée sur les événements d’achat


## Quality Checks and Plans
On réalise des contrôles simples :
- comptage de lignes
- vérification des valeurs nulles
- couverture référentielle entre faits et dimensions



