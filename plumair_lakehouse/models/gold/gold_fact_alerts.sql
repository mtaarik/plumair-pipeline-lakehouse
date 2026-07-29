{{ config(
    materialized='incremental',
    unique_key='alert_id',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT raw_payload, kafka_offset, kafka_ingestion_ts
    FROM {{ ref('bronze_alerts_raw') }}
    {% if is_incremental() %}
        WHERE kafka_offset > (SELECT COALESCE(MAX(kafka_offset), -1) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        CAST(json_extract_scalar(raw_payload, '$.StationId') AS VARCHAR) AS station_id,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Datetime') AS TIMESTAMP) AS triggered_at,
        TRY_CAST(json_extract_scalar(raw_payload, '$["PM2.5"]') AS DOUBLE) AS pm25_value,
        CAST(json_extract_scalar(raw_payload, '$.PM25_Category_India') AS VARCHAR) AS aqi_category_at_trigger,
        kafka_offset
    FROM raw
    WHERE json_extract_scalar(raw_payload, '$.StationId') IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY station_id, triggered_at ORDER BY kafka_offset DESC) AS rn
    FROM parsed
)

SELECT
    to_hex(md5(to_utf8(CONCAT(station_id, '_', CAST(triggered_at AS VARCHAR), '_', CAST(kafka_offset AS VARCHAR))))) AS alert_id,
    station_id,
    triggered_at,
    pm25_value,
    aqi_category_at_trigger,
    155.1 AS threshold_applied,
    TRUE AS threshold_exceeded
FROM deduplicated
WHERE rn = 1