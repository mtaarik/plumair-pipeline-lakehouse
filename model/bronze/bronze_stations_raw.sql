{{ config(
    materialized='table'
) }}

SELECT 
    CAST(_message AS VARCHAR) AS raw_payload,
    _timestamp AS kafka_ingestion_ts,
    _partition_id AS kafka_partition,
    _offset AS kafka_offset
FROM kafka.default."iot-sensors-raw"

