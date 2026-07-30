{{ config(
    materialized='incremental',
    unique_key=['station_id', 'measurement_time'],
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('silver_telemetry') }}
    {% if is_incremental() %}
        WHERE ingestion_timestamp > (
            SELECT COALESCE(MAX(ingestion_timestamp), TIMESTAMP '1970-01-01') FROM {{ this }}
        )
    {% endif %}
)

SELECT
    d.station_sk,
    t.station_id,
    t.measurement_time,
    CAST(t.measurement_time AS DATE) AS date_id,

    -- Météo
    t.temperature_c, t.temperature_is_outlier,
    t.humidity_percent, t.humidity_is_outlier,
    t.wind_speed_kmh, t.wind_speed_is_outlier,
    t.wind_gusts_kmh, t.wind_gusts_is_outlier,
    t.pressure_msl_hpa, t.pressure_is_outlier,
    t.precipitation_mm, t.precipitation_is_outlier,
    t.cloud_cover_percent, t.cloud_cover_is_outlier,

    -- Polluants (jeu complet, requis par gold_fact_compliance et les dashboards multi-polluants)
    t.pm25_ugm3, t.pm25_is_outlier,
    t.pm10_ugm3, t.pm10_is_outlier,
    t.co_ugm3, t.co_is_outlier,
    t.no2_ugm3, t.no2_is_outlier,
    t.so2_ugm3, t.so2_is_outlier,
    t.o3_ugm3, t.o3_is_outlier,
    t.dust_ugm3, t.dust_is_outlier,
    t.aod,

    -- Contexte (justifie le pivot de dataset, section 5.3)
    t.temp_inversion,
    t.festival_period,
    t.crop_burning_season,

    -- Traçabilité, non-officielle (voir gold_fact_compliance pour la classification officielle)
    t.pm25_category_india_source,

    -- Fraîcheur de la ligne, utile pour du monitoring de pipeline côté BI
    CURRENT_TIMESTAMP AS _loaded_at
    c.category_id AS aqi_category_id,

FROM source t
LEFT JOIN {{ ref('gold_dim_station') }} d
    ON t.station_id = d.station_id
    AND d.is_current = TRUE
LEFT JOIN {{ ref('gold_dim_aqi_category') }} c
    ON t.pm25_ugm3 BETWEEN c.pm25_min AND c.pm25_max
