{{ config(
    materialized='incremental',
    unique_key=['station_id', 'measurement_time'],
    incremental_strategy='merge'
) }}

WITH raw_data AS (
    SELECT raw_payload, kafka_ingestion_ts, kafka_offset
    FROM {{ ref('bronze_stations_raw') }}
    {% if is_incremental() %}
        WHERE kafka_offset > (SELECT COALESCE(MAX(kafka_offset), -1) FROM {{ this }})
    {% endif %}
),

parsed_data AS (
    SELECT
        CAST(json_extract_scalar(raw_payload, '$.StationId') AS VARCHAR) AS station_id,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Datetime') AS TIMESTAMP) AS measurement_time,

        -- Météo (mesures brutes)
        TRY_CAST(json_extract_scalar(raw_payload, '$.Temp_2m_C') AS DOUBLE) AS raw_temperature,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Humidity_Percent') AS DOUBLE) AS raw_humidity,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Dew_Point_C') AS DOUBLE) AS dew_point_c,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Wind_Speed_10m_kmh') AS DOUBLE) AS raw_wind_speed,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Wind_Dir_10m') AS DOUBLE) AS wind_dir_10m,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Wind_Gusts_kmh') AS DOUBLE) AS raw_wind_gusts,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Precipitation_mm') AS DOUBLE) AS raw_precipitation,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Rain_mm') AS DOUBLE) AS rain_mm,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Pressure_MSL_hPa') AS DOUBLE) AS raw_pressure,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Surface_Pressure_hPa') AS DOUBLE) AS surface_pressure_hpa,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Solar_Radiation_Wm2') AS DOUBLE) AS solar_radiation_wm2,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Direct_Radiation_Wm2') AS DOUBLE) AS direct_radiation_wm2,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Diffuse_Radiation_Wm2') AS DOUBLE) AS diffuse_radiation_wm2,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Cloud_Cover_Percent') AS DOUBLE) AS raw_cloud_cover,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Cloud_Low_Percent') AS DOUBLE) AS cloud_low_percent,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Cloud_Mid_Percent') AS DOUBLE) AS cloud_mid_percent,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Cloud_High_Percent') AS DOUBLE) AS cloud_high_percent,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Sunshine_Seconds') AS DOUBLE) AS sunshine_seconds,

        -- Polluants (mesures brutes)
        TRY_CAST(json_extract_scalar(raw_payload, '$["PM2.5"]') AS DOUBLE) AS pm25_ugm3,
        TRY_CAST(json_extract_scalar(raw_payload, '$.PM10_ugm3') AS DOUBLE) AS pm10_ugm3,
        TRY_CAST(json_extract_scalar(raw_payload, '$.CO_ugm3') AS DOUBLE) AS co_ugm3,
        TRY_CAST(json_extract_scalar(raw_payload, '$.NO2_ugm3') AS DOUBLE) AS raw_no2,
        TRY_CAST(json_extract_scalar(raw_payload, '$.SO2_ugm3') AS DOUBLE) AS raw_so2,
        TRY_CAST(json_extract_scalar(raw_payload, '$.O3_ugm3') AS DOUBLE) AS raw_o3,
        TRY_CAST(json_extract_scalar(raw_payload, '$.Dust_ugm3') AS DOUBLE) AS raw_dust,
        TRY_CAST(json_extract_scalar(raw_payload, '$.AOD') AS DOUBLE) AS aod,

        -- Signaux de contexte externes (non dérivables d'une autre colonne, à capter tels quels)
        CAST(json_extract_scalar(raw_payload, '$.Wind_Stagnation') AS BOOLEAN) AS wind_stagnation,
        CAST(json_extract_scalar(raw_payload, '$.Is_Raining') AS BOOLEAN) AS is_raining,
        CAST(json_extract_scalar(raw_payload, '$.Heavy_Rain') AS BOOLEAN) AS heavy_rain,
        CAST(json_extract_scalar(raw_payload, '$.Temp_Inversion') AS BOOLEAN) AS temp_inversion,
        CAST(json_extract_scalar(raw_payload, '$.Festival_Period') AS BOOLEAN) AS festival_period,
        CAST(json_extract_scalar(raw_payload, '$.Crop_Burning_Season') AS BOOLEAN) AS crop_burning_season,

        -- Traçabilité source (propagée telle quelle, pas recalculée en Silver)
        CAST(json_extract_scalar(raw_payload, '$.PM25_Category_India') AS VARCHAR) AS pm25_category_india_source,

        kafka_ingestion_ts AS ingestion_timestamp,
        kafka_offset
    FROM raw_data
    WHERE json_extract_scalar(raw_payload, '$.StationId') IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY station_id, measurement_time
            ORDER BY kafka_offset DESC
        ) AS rn
    FROM parsed_data
)

SELECT
    station_id,
    measurement_time,

    -- Météo, avec flags de qualité sur les plages physiquement plausibles
    CASE WHEN raw_temperature BETWEEN -50 AND 60 THEN raw_temperature ELSE NULL END AS temperature_c,
    CASE WHEN raw_temperature IS NOT NULL AND raw_temperature NOT BETWEEN -50 AND 60 THEN TRUE ELSE FALSE END AS temperature_is_outlier,

    CASE WHEN raw_humidity BETWEEN 0 AND 100 THEN raw_humidity ELSE NULL END AS humidity_percent,
    CASE WHEN raw_humidity IS NOT NULL AND raw_humidity NOT BETWEEN 0 AND 100 THEN TRUE ELSE FALSE END AS humidity_is_outlier,

    dew_point_c,

    CASE WHEN raw_wind_speed >= 0 THEN raw_wind_speed ELSE NULL END AS wind_speed_kmh,
    CASE WHEN raw_wind_speed IS NOT NULL AND raw_wind_speed < 0 THEN TRUE ELSE FALSE END AS wind_speed_is_outlier,
    wind_dir_10m,
    CASE WHEN raw_wind_gusts >= 0 THEN raw_wind_gusts ELSE NULL END AS wind_gusts_kmh,
    CASE WHEN raw_wind_gusts IS NOT NULL AND raw_wind_gusts < 0 THEN TRUE ELSE FALSE END AS wind_gusts_is_outlier,

    CASE WHEN raw_precipitation >= 0 THEN raw_precipitation ELSE NULL END AS precipitation_mm,
    CASE WHEN raw_precipitation IS NOT NULL AND raw_precipitation < 0 THEN TRUE ELSE FALSE END AS precipitation_is_outlier,
    rain_mm,

    CASE WHEN raw_pressure BETWEEN 850 AND 1100 THEN raw_pressure ELSE NULL END AS pressure_msl_hpa,
    CASE WHEN raw_pressure IS NOT NULL AND raw_pressure NOT BETWEEN 850 AND 1100 THEN TRUE ELSE FALSE END AS pressure_is_outlier,
    surface_pressure_hpa,

    solar_radiation_wm2,
    direct_radiation_wm2,
    diffuse_radiation_wm2,

    CASE WHEN raw_cloud_cover BETWEEN 0 AND 100 THEN raw_cloud_cover ELSE NULL END AS cloud_cover_percent,
    CASE WHEN raw_cloud_cover IS NOT NULL AND raw_cloud_cover NOT BETWEEN 0 AND 100 THEN TRUE ELSE FALSE END AS cloud_cover_is_outlier,
    cloud_low_percent,
    cloud_mid_percent,
    cloud_high_percent,
    sunshine_seconds,

    -- Polluants non observés négatifs dans l'EDA initiale, mais flag défensif conservé par cohérence
    pm25_ugm3,
    CASE WHEN pm25_ugm3 IS NOT NULL AND pm25_ugm3 < 0 THEN TRUE ELSE FALSE END AS pm25_is_outlier,
    pm10_ugm3,
    CASE WHEN pm10_ugm3 IS NOT NULL AND pm10_ugm3 < 0 THEN TRUE ELSE FALSE END AS pm10_is_outlier,
    co_ugm3,
    CASE WHEN co_ugm3 IS NOT NULL AND co_ugm3 < 0 THEN TRUE ELSE FALSE END AS co_is_outlier,
    aod,

    -- Polluants gazeux & poussière : neutre, décision reportée en aval (remplacement par NULL si < 0)
    CASE WHEN raw_no2 >= 0 THEN raw_no2 ELSE NULL END AS no2_ugm3,
    CASE WHEN raw_no2 IS NOT NULL AND raw_no2 < 0 THEN TRUE ELSE FALSE END AS no2_is_outlier,

    CASE WHEN raw_so2 >= 0 THEN raw_so2 ELSE NULL END AS so2_ugm3,
    CASE WHEN raw_so2 IS NOT NULL AND raw_so2 < 0 THEN TRUE ELSE FALSE END AS so2_is_outlier,

    CASE WHEN raw_o3 >= 0 THEN raw_o3 ELSE NULL END AS o3_ugm3,
    CASE WHEN raw_o3 IS NOT NULL AND raw_o3 < 0 THEN TRUE ELSE FALSE END AS o3_is_outlier,

    CASE WHEN raw_dust >= 0 THEN raw_dust ELSE NULL END AS dust_ugm3,
    CASE WHEN raw_dust IS NOT NULL AND raw_dust < 0 THEN TRUE ELSE FALSE END AS dust_is_outlier,

    -- Signaux de contexte externes, non dérivables, propagés tels quels
    wind_stagnation,
    is_raining,
    heavy_rain,
    temp_inversion,
    festival_period,
    crop_burning_season,

    -- Traçabilité, non recalculée ici (le recalcul officiel se fait en Gold via gold_dim_pollution_threshold)
    pm25_category_india_source,

    ingestion_timestamp,
    kafka_offset

FROM deduplicated
WHERE rn = 1