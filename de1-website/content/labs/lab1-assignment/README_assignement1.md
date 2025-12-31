# Assignment 1 – Report

## Objective
L’objectif de ce travail est de lancer un environnement PySpark fonctionnel
dans JupyterLab et de réaliser un word count simple en utilisant deux approches :
RDD et DataFrame.

On a produit deux résultats :
- un Top-10 des mots incluant les stopwords
- un Top-10 des mots excluant les stopwords

Les résultats sont exportés sous forme de fichiers CSV.

## Inputs
- Dataset : `a1-brand.csv` (fourni)
- Stopwords : liste définie dans le notebook

Les variables d’environnement SPARK_HOME et PATH ont été correctement configurées.

## Method
On a implémenté un word count minimal :
- une version basée sur les RDD
- une version basée sur les DataFrames

Les deux versions suivent la même logique afin de permettre la comparaison.

## Outputs
- `top10_words.csv` : Top-10 incluant les stopwords
- `top10_noStopWords.csv` : Top-10 sans stopwords

