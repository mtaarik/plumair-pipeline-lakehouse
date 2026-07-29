{% snapshot station_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='station_id',
        strategy='check',
        check_cols=['state', 'latitude', 'longitude']
    )
}}

SELECT
    station_id,
    state,
    latitude,
    longitude
FROM {{ ref('silver_station') }}

{% endsnapshot %}