{{ config(
    materialized='incremental',
    unique_key=['station_id', 'measurement_time'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('silver_telemetry') }}
    {% if is_incremental() %}
        -- Watermark sur l'horodatage Kafka propagé depuis Silver (1 minute de recouvrement,
        -- merge idempotent). Un curseur sur _loaded_at, horloge du run, perdrait les messages
        -- arrivés dans Kafka pendant l'exécution d'un run.
        WHERE ingestion_timestamp >= (
            SELECT COALESCE(MAX(_meta_kafka_ingestion_ts), TIMESTAMP '1970-01-01') - INTERVAL '1' MINUTE
            FROM {{ this }}
        )
    {% endif %}
),

-- Pré-calcul de la première date de validité pour contourner la limitation de Trino
dim_station_enhanced AS (
    SELECT 
        *,
        MIN(valid_from) OVER (PARTITION BY station_id) AS first_valid_from
    FROM {{ ref('gold_dim_station') }}
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

    -- Polluants
    t.pm25_ugm3, t.pm25_is_outlier,
    t.pm10_ugm3, t.pm10_is_outlier,
    t.co_ugm3, t.co_is_outlier,
    t.no2_ugm3, t.no2_is_outlier,
    t.so2_ugm3, t.so2_is_outlier,
    t.o3_ugm3, t.o3_is_outlier,
    t.dust_ugm3, t.dust_is_outlier,
    t.aod,

    -- Contexte
    t.temp_inversion,
    t.festival_period,
    t.crop_burning_season,

    -- Traçabilité
    t.pm25_category_india_source,

    -- Catégorie AQI issue de la dimension
    c.category_id AS aqi_category_id,

    -- Lignage technique (à masquer dans Power BI) : curseur incrémental et mesure de fraîcheur
    t.meta_ingested_at AS _meta_ingested_at,
    t.ingestion_timestamp AS _meta_kafka_ingestion_ts,

    -- Fraîcheur de la ligne (Corrigé pour Iceberg)
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS _loaded_at

FROM source t
LEFT JOIN dim_station_enhanced d
    ON t.station_id = d.station_id
    AND (
        t.measurement_time >= d.valid_from 
        OR d.valid_from = d.first_valid_from
    )
    AND (d.valid_to IS NULL OR t.measurement_time < d.valid_to)
LEFT JOIN {{ ref('gold_dim_aqi_category') }} c
    ON t.pm25_ugm3 BETWEEN c.pm25_min AND c.pm25_max