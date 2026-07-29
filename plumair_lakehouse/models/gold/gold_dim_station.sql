{{ config(materialized='table') }}

SELECT
    -- Clé de substitution unique basée sur l'ID et la date de début de version
    to_hex(md5(to_utf8(CONCAT(station_id, '_', CAST(dbt_valid_from AS VARCHAR))))) AS station_sk,
    station_id,
    state,
    latitude,
    longitude,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM {{ ref('station_snapshot') }}