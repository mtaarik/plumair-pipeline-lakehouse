{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT raw_payload
    FROM {{ ref('bronze_stations_raw') }}
),

parsed_stations AS (
    SELECT DISTINCT
        CAST(json_extract_scalar(raw_payload, '$.StationId') AS VARCHAR) AS station_id,
        CAST(json_extract_scalar(raw_payload, '$.State') AS VARCHAR) AS state,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Latitude') AS DOUBLE) AS latitude,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Longitude') AS DOUBLE) AS longitude
    FROM raw_data
    WHERE json_extract_scalar(raw_payload, '$.StationId') IS NOT NULL
)

SELECT
    station_id,
    state,
    latitude,
    longitude
FROM parsed_stations