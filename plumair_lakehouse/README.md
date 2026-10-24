# plumair_lakehouse (dbt)

Projet dbt de la plateforme Plum'Air, exécuté sur Trino (adaptateur `dbt-trino`) au-dessus du
catalogue Iceberg. Voir le README à la racine du dépôt pour l'architecture complète.

| Dossier | Contenu |
|---|---|
| `models/bronze` | Lecture des topics Kafka, append dans Iceberg (partitionné par jour) |
| `models/silver` | Référentiel stations et télémétrie horaire nettoyée (merge, flags de qualité) |
| `models/gold` | Dimensions, faits, KPI mensuels et vue ML |
| `snapshots` | Historisation SCD Type 2 du référentiel stations |
| `seeds` | Catégories AQI (CPCB), seuils réglementaires (CPCB / OMS), opérateurs synthétiques |
| `macros` | `generate_schema_name` : un schéma physique par couche |
| `analyses` | Requêtes de démonstration (time travel Iceberg) |

```bash
dbt deps
dbt seed
dbt run --select bronze silver
dbt snapshot
dbt run --select gold
dbt test
```

En production, ces étapes sont enchaînées toutes les 5 minutes par le DAG Airflow
`dbt_lakehouse_pipeline` (`infrastructure/airflow/dags`).
