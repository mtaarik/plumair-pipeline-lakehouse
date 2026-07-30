{{ config(materialized='view') }}

SELECT
    t.station_id,
    s.state,
    s.latitude,
    s.longitude,

    t.measurement_time,

    -- Météo (features d'entrée)
    t.temperature_c,
    t.humidity_percent,
    t.dew_point_c,
    t.wind_speed_kmh,
    t.wind_dir_10m,
    t.wind_gusts_kmh,
    t.pressure_msl_hpa,
    t.precipitation_mm,
    t.cloud_cover_percent,
    t.solar_radiation_wm2,

    -- Contexte (features catégorielles/booléennes, justifiant le pivot de dataset)
    t.wind_stagnation,
    t.temp_inversion,
    t.festival_period,
    t.crop_burning_season,

    -- Cibles potentielles / features de pollution
    t.pm25_ugm3,
    t.pm10_ugm3,
    t.co_ugm3,
    t.no2_ugm3,
    t.so2_ugm3,
    t.o3_ugm3,
    t.dust_ugm3,
    t.aod,

    -- Filtre qualité laissé à la discrétion du Data Scientist, pas appliqué ici
    t.pm25_is_outlier,
    t.temperature_is_outlier,
    t.humidity_is_outlier

FROM {{ ref('silver_telemetry') }} t
LEFT JOIN {{ ref('silver_station') }} s
    ON t.station_id = s.station_id