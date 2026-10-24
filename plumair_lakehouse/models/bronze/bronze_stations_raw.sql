{{ config(
    materialized='incremental',
    incremental_strategy='append',
    properties={"partitioning": "ARRAY['day(kafka_ingestion_ts)']"}
) }}

-- Bronze : copie brute et immuable du topic Kafka (append-only).
-- L'historique survit à la rétention Kafka : on n'ajoute que les messages jamais lus.
-- Le topic n'a qu'une partition (cf. kafka-init) : l'offset est donc un curseur total.
SELECT
    CAST(_message AS VARCHAR) AS raw_payload,
    CAST(_timestamp AS TIMESTAMP(6)) AS kafka_ingestion_ts,  -- Iceberg n'accepte que timestamp(6)
    _partition_id AS kafka_partition,
    _partition_offset AS kafka_offset
FROM kafka.default."iot-sensors-raw"

{% if is_incremental() %}
    -- Offset : curseur principal. Timestamp : filet de sécurité si le topic est recréé
    -- (les offsets repartent alors en arrière, mais les nouveaux messages sont plus récents).
    WHERE _partition_offset > (SELECT COALESCE(MAX(kafka_offset), -1) FROM {{ this }})
       OR _timestamp > (SELECT COALESCE(MAX(kafka_ingestion_ts), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
