{{ config(
    materialized='incremental',
    unique_key='alert_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT alert_id, station_id, triggered_at
    FROM {{ ref('gold_fact_alerts') }}
    {% if is_incremental() %}
        WHERE alert_id NOT IN (SELECT alert_id FROM {{ this }})
    {% endif %}
),

nb_operators AS (
    SELECT COUNT(*) AS n FROM {{ ref('gold_dim_operator') }}
),

hashed AS (
    SELECT
        s.alert_id,
        s.station_id,
        s.triggered_at,

        -- Pseudo-aléatoire déterministe basé sur le hash de alert_id : reproductible à chaque run
        abs(from_big_endian_64(xxhash64(to_utf8(s.alert_id)))) AS hash_value,
        n.n AS nb_op
    FROM source s
    CROSS JOIN nb_operators n
)

SELECT
    h.alert_id,
    h.station_id,
    h.triggered_at,

    o.operator_id,
    o.operator_name,

    -- Temps de réponse simulé : 2 à 45 minutes, dérivé du hash (pas de RANDOM() pour rester stable au fil des runs)
    2 + (h.hash_value % 44) AS response_time_minutes,

    h.triggered_at + (INTERVAL '1' MINUTE * (2 + (h.hash_value % 44))) AS resolved_at,

    TRUE AS is_synthetic

FROM hashed h
JOIN {{ ref('gold_dim_operator') }} o
    ON o.operator_id = CONCAT('OP', LPAD(CAST((h.hash_value % h.nb_op) + 1 AS VARCHAR), 3, '0'))