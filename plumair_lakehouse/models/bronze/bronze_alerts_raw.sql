{{ config(materialized='table') }}

SELECT
    _message AS raw_payload,
    _partition_offset AS kafka_offset,
    _timestamp AS kafka_ingestion_ts
FROM kafka.default."iot-emissions-alert"