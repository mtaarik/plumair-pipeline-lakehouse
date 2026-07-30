{{ config(materialized='table') }}

SELECT
    r.station_id,
    o.operator_id,
    o.operator_name,
    o.shift,

    COUNT(*) AS nb_alertes_traitees,
    ROUND(AVG(r.response_time_minutes), 1) AS temps_reponse_moyen_min,
    MIN(r.response_time_minutes) AS temps_reponse_min,
    MAX(r.response_time_minutes) AS temps_reponse_max

FROM {{ ref('gold_fact_alert_response') }} r
JOIN {{ ref('gold_dim_operator') }} o
    ON r.operator_id = o.operator_id

GROUP BY r.station_id, o.operator_id, o.operator_name, o.shift