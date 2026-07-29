{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    ) }}
)

SELECT
    cast(date_day as date) AS date_id,
    year(date_day) AS year,
    month(date_day) AS month,
    day(date_day) AS day,
    day_of_week(date_day) AS day_of_week,
    date_format(date_day, '%W') AS day_name,
    week(date_day) AS week_of_year,
    quarter(date_day) AS quarter,
    CASE WHEN day_of_week(date_day) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,

    -- Saisons indiennes (IMD), alignées sur la logique déjà présente dans le dataset source
    CASE
        WHEN month(date_day) IN (12, 1, 2) THEN 'Winter'
        WHEN month(date_day) IN (3, 4, 5) THEN 'Summer'
        WHEN month(date_day) IN (6, 7, 8, 9) THEN 'Monsoon'
        WHEN month(date_day) IN (10, 11) THEN 'Post_Monsoon'
    END AS season

FROM date_spine