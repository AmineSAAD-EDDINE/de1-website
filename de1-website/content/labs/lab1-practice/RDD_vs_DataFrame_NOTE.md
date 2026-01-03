# RDD vs DataFrame – Engineering Note (Lab 1)

>**Students:** Maxence DELEHELLE, Amine SAAD-EDDINE   
>**Teacher:** Badr TAJINI     
>**Academic year:** 2025–2026  
>**Program:** Data & Applications - Engineering - (FD)   
>**Course:** Data Engineering I  
---

Dans ce lab, on a implémenté le même calcul de Top-N en utilisant deux approches :
une avec les RDD et une avec les DataFrames, afin de comparer leur comportement
et leurs implications en ingénierie des données.

Avec les RDD, on utilise une API bas niveau où chaque transformation
(map, reduceByKey, sortBy) est définie explicitement.
Cette approche offre plus de contrôle sur le traitement, mais elle ne bénéficie
d’aucune optimisation automatique.
Dans le Spark UI, on observe des stages bien distincts avec des shuffles explicites,
et un plan d’exécution directement imposé par le code.

Avec les DataFrames, on adopte une API haut niveau et déclarative.
On décrit ce que l’on veut calculer, et Spark se charge de l’optimisation grâce
au Catalyst Optimizer et au moteur Tungsten.
Les plans logique et physique générés sont plus optimisés, et le Spark UI montre
une exécution plus efficace pour le même calcul de Top-N.

En pratique, ce lab montre que les DataFrames sont mieux adaptés aux traitements
analytiques structurés grâce à leurs optimisations automatiques et à leur
meilleure performance.
Les RDD restent utiles lorsque l’on a besoin d’un contrôle fin ou de traitements
spécifiques non structurés.

Les observations sont cohérentes avec les plans sauvegardés et les métriques
enregistrées dans le fichier [lab1_metrics_log.csv](../../../quartz/static/lab1-practice/lab1_metrics_log.csv).

