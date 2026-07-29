{{ config(materialized='table') }}

WITH pollutants_long AS (
    SELECT station_id, CAST(measurement_time AS DATE) AS date_id, 'PM2.5' AS pollutant, pm25_ugm3 AS value FROM {{ ref('silver_telemetry') }}
    UNION ALL
    SELECT station_id, CAST(measurement_time AS DATE), 'PM10', pm10_ugm3 FROM {{ ref('silver_telemetry') }}
    UNION ALL
    SELECT station_id, CAST(measurement_time AS DATE), 'NO2', no2_ugm3 FROM {{ ref('silver_telemetry') }}
    UNION ALL
    SELECT station_id, CAST(measurement_time AS DATE), 'SO2', so2_ugm3 FROM {{ ref('silver_telemetry') }}
),

daily_avg AS (
    SELECT
        station_id,
        date_id,
        pollutant,
        AVG(value) AS value_avg_24h,
        COUNT(value) AS nb_mesures
    FROM pollutants_long
    WHERE value IS NOT NULL
    GROUP BY station_id, date_id, pollutant
    HAVING COUNT(value) >= 20  -- couverture minimale pour représentativité du jour (sur 24h attendues)
)

SELECT
    d.station_id,
    d.date_id,
    d.pollutant,
    '24h' AS averaging_period,
    d.value_avg_24h,
    d.nb_mesures,
    t.india_limit_industrial AS threshold_applied,
    CASE WHEN d.value_avg_24h > t.india_limit_industrial THEN TRUE ELSE FALSE END AS depassement
FROM daily_avg d
JOIN {{ ref('gold_dim_pollution_threshold') }} t
    ON t.pollutant = d.pollutant AND t.averaging_period = '24h'