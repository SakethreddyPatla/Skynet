{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='kpi_surrogate_key'
) }}

WITH new_silver_events AS (
    SELECT *
    FROM {{ ref('stg_flights') }}

    {% if is_incremental() %}
        
        WHERE ingested_at_utc > (SELECT MAX(latest_ingested_at_utc) FROM {{ this }})
    {% endif %}
),

country_aggregations AS (
    SELECT
        date_trunc('hour', time_position_utc) AS batch_window_utc,
        COALESCE(origin_country, 'UNKNOWN') AS origin_country,
        on_ground,
        
        -- Aggregate Metrics for the incremental window
        COUNT(DISTINCT icao24) AS total_active_aircraft,
        ROUND(AVG(velocity), 2) AS avg_velocity_mps,
        ROUND(AVG(baro_altitude), 2) AS avg_barometric_altitude_m,
        ROUND(MAX(velocity), 2) AS max_velocity_mps,
        
        -- Watermark to keep track of the max Silver timestamp processed in this window
        MAX(ingested_at_utc) AS latest_ingested_at_utc
    FROM new_silver_events
    GROUP BY 
        date_trunc('hour', time_position_utc),
        COALESCE(origin_country, 'UNKNOWN'),
        on_ground
)

SELECT
    -- primary surrogate key for Delta Lake MERGE operations
    md5(concat(
        cast(batch_window_utc as string), '_',
        origin_country, '_',
        cast(on_ground as string)
    )) AS kpi_surrogate_key,
    batch_window_utc,
    origin_country,
    on_ground,
    total_active_aircraft,
    avg_velocity_mps,
    avg_barometric_altitude_m,
    max_velocity_mps,
    latest_ingested_at_utc,
    current_timestamp() AS aggregated_at_utc
FROM country_aggregations