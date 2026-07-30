{{ config(materialized='table') }}

SELECT
    c.station_id,
    d.year,
    d.month,
    c.pollutant,
    c.averaging_period,

    COUNT(*) AS nb_jours_mesures,
    SUM(CASE WHEN c.depassement THEN 1 ELSE 0 END) AS nb_jours_depassement,

    ROUND(
        SUM(CASE WHEN c.depassement THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS taux_depassement_pct,

    ROUND(AVG(c.value_avg_24h), 2) AS moyenne_mensuelle,
    ROUND(MAX(c.value_avg_24h), 2) AS pic_mensuel,

    MAX(c.threshold_applied) AS seuil_reglementaire_applique

FROM {{ ref('gold_fact_compliance') }} c
JOIN {{ ref('gold_dim_date') }} d
    ON c.date_id = d.date_id

GROUP BY
    c.station_id, d.year, d.month, c.pollutant, c.averaging_period