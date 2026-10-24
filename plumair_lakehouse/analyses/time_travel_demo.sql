-- Démonstration du Time Travel Apache Iceberg (à exécuter dans Trino)
-- dbt compile ce fichier sans le matérialiser : dbt compile --select time_travel_demo

-- 1. Registre des commits de la table de faits (un snapshot par écriture dbt).
--    total-records augmente plus vite que le nombre de lignes : le merge écrit des
--    fichiers de suppression (Merge-on-Read) au lieu de réécrire les fichiers Parquet.
SELECT committed_at, snapshot_id, operation,
       summary['added-records'] AS added_records,
       summary['total-records'] AS total_records
FROM iceberg.gold."gold_fact_air_quality$snapshots"
ORDER BY committed_at;

-- 2. Lecture d'une version antérieure, sans sauvegarde externe.
--    Remplacer l'identifiant par un snapshot_id renvoyé par la requête 1.
SELECT 'snapshot initial' AS version, count(*) AS lignes,
       count(_meta_ingested_at) AS lignes_rejouees, round(avg(pm25_ugm3), 2) AS pm25_moyen
FROM {{ ref('gold_fact_air_quality') }} FOR VERSION AS OF 8984442794094147591
UNION ALL
SELECT 'version courante', count(*), count(_meta_ingested_at), round(avg(pm25_ugm3), 2)
FROM {{ ref('gold_fact_air_quality') }};
