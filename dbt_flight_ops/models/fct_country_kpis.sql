{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

with silver_flights as (
    select * from {{ ref('stg_flights') }}
)

select
    origin_country,
    on_ground,
    count(distinct icao24) as total_active_aircraft,
    ROUND(AVG(velocity), 2) AS avg_velocity_ms,
    ROUND(AVG(baro_altitude), 2) AS avg_altitude_meters,
    ROUND(AVG(transponder_delay_seconds), 2) AS avg_signal_delay_sec,
    CURRENT_TIMESTAMP() AS gold_updated_at_utc
FROM silver_flights
GROUP BY 
    origin_country, 
    on_ground