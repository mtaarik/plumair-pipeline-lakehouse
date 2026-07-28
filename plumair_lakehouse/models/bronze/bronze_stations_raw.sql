{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT 
    CAST(_message AS VARCHAR) AS raw_payload,
    _timestamp AS kafka_ingestion_ts,
    _partition_id AS kafka_partition,
    _partition_offset AS kafka_offset
FROM kafka.default."iot-sensors-raw"

{% if is_incremental() %}
    WHERE _partition_offset > (SELECT COALESCE(MAX(kafka_offset), -1) FROM {{ this }})
{% endif %}