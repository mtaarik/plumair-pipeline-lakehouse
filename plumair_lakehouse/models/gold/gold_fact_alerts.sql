{{ config(
    materialized='incremental',
    unique_key='alert_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH raw AS (
    SELECT raw_payload, kafka_offset, kafka_ingestion_ts
    FROM {{ ref('bronze_alerts_raw') }}
    {% if is_incremental() %}
        WHERE kafka_ingestion_ts >= (
            SELECT COALESCE(MAX(_meta_kafka_ingestion_ts), TIMESTAMP '1970-01-01') - INTERVAL '1' MINUTE
            FROM {{ this }}
        )
    {% endif %}
),

parsed AS (
    SELECT
        CAST(json_extract_scalar(raw_payload, '$.StationId') AS VARCHAR) AS station_id,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Datetime') AS TIMESTAMP(6)) AS triggered_at,
        TRY_CAST(json_extract_scalar(raw_payload, '$["PM2.5"]') AS DOUBLE) AS pm25_value,
        CAST(json_extract_scalar(raw_payload, '$.PM25_Category_India') AS VARCHAR) AS aqi_category_at_trigger,
        TRY_CAST(json_extract_scalar(raw_payload, '$._meta_ingested_at') AS TIMESTAMP(6)) AS meta_ingested_at,
        kafka_ingestion_ts,
        kafka_offset
    FROM raw
    WHERE json_extract_scalar(raw_payload, '$.StationId') IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY station_id, triggered_at ORDER BY kafka_ingestion_ts DESC, kafka_offset DESC) AS rn
    FROM parsed
)

SELECT
    -- Clé métier (station, instant de mesure) : un rejeu du flux produit le même alert_id,
    -- le merge met donc l'alerte à jour au lieu de la dupliquer.
    to_hex(md5(to_utf8(CONCAT(station_id, '_', CAST(triggered_at AS VARCHAR))))) AS alert_id,
    station_id,
    triggered_at,
    pm25_value,
    aqi_category_at_trigger,
    155.1 AS threshold_applied,
    TRUE AS threshold_exceeded,

    -- Lignage technique : horodatage NiFi et arrivée dans Kafka (watermark incrémental)
    meta_ingested_at AS _meta_ingested_at,
    kafka_ingestion_ts AS _meta_kafka_ingestion_ts
FROM deduplicated
WHERE rn = 1