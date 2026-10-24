{{ config(
    materialized='incremental',
    incremental_strategy='append',
    properties={"partitioning": "ARRAY['day(kafka_ingestion_ts)']"}
) }}

-- Même principe que bronze_stations_raw : append-only sur le curseur d'offset,
-- pour que les alertes déjà archivées ne disparaissent pas avec la rétention Kafka.
SELECT
    _message AS raw_payload,
    _partition_offset AS kafka_offset,
    CAST(_timestamp AS TIMESTAMP(6)) AS kafka_ingestion_ts  -- Iceberg n'accepte que timestamp(6)
FROM kafka.default."iot-emissions-alert"

{% if is_incremental() %}
    -- Offset : curseur principal. Timestamp : filet de sécurité si le topic est recréé
    -- (les offsets repartent alors en arrière, mais les nouveaux messages sont plus récents).
    WHERE _partition_offset > (SELECT COALESCE(MAX(kafka_offset), -1) FROM {{ this }})
       OR _timestamp > (SELECT COALESCE(MAX(kafka_ingestion_ts), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
