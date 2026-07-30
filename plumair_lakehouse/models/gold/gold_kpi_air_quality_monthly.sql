{{ config(materialized='table') }}

SELECT
    a.station_id,
    d.year,
    d.month,

    ROUND(AVG(a.pm25_ugm3), 2) AS pm25_avg,
    ROUND(MAX(a.pm25_ugm3), 2) AS pm25_max,
    ROUND(AVG(a.pm10_ugm3), 2) AS pm10_avg,
    ROUND(AVG(a.no2_ugm3), 2) AS no2_avg,
    ROUND(AVG(a.so2_ugm3), 2) AS so2_avg,
    ROUND(AVG(a.o3_ugm3), 2) AS o3_avg,

    -- Part du temps en conditions défavorables à la dispersion des polluants
    ROUND(SUM(CASE WHEN a.temp_inversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_heures_inversion_thermique,
    ROUND(SUM(CASE WHEN a.crop_burning_season THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_heures_saison_brulis,

    -- Suivi de la qualité de la donnée elle-même, exposé au BI
    COUNT(*) AS nb_mesures_horaires,
    SUM(CASE WHEN a.pm25_is_outlier THEN 1 ELSE 0 END) AS nb_valeurs_pm25_rejetees

FROM {{ ref('gold_fact_air_quality') }} a
JOIN {{ ref('gold_dim_date') }} d
    ON a.date_id = d.date_id

GROUP BY
    a.station_id, d.year, d.month