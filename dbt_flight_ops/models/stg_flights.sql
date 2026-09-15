{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='flight_event_id',
    pre_hook="""
        COPY INTO skynet.default.flight_ops_bronze
        FROM '/Volumes/skynet/default/raw_open_sky_landing/'
        FILEFORMAT = JSON;
    """
) }}

WITH raw_bronze AS (
    SELECT * FROM {{ source('skynet_source', 'flight_ops_bronze') }}
    
    {% if is_incremental() %}
        -- Only process records ingested since the max timestamp already in stg_flights
        WHERE to_timestamp(ingestion_timestamp_utc) > (SELECT MAX(ingested_at_utc) FROM {{ this }})
    {% endif %}
),

deduplicated_bronze AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, time_position 
            ORDER BY ingestion_timestamp_utc DESC
        ) AS row_num
    FROM raw_bronze
    WHERE icao24 IS NOT NULL
      AND time_position IS NOT NULL
)

SELECT
    md5(concat(coalesce(icao24, ''), '_', cast(time_position as string))) AS flight_event_id,
    icao24,
    TRIM(callsign) AS callsign,
    origin_country,
    to_timestamp(CAST(time_position AS LONG)) AS time_position_utc,
    to_timestamp(CAST(last_contact AS LONG)) AS last_contact_utc,
    (last_contact - time_position) AS transponder_delay_seconds,
    latitude,
    longitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    batch_id,
    to_timestamp(ingestion_timestamp_utc) AS ingested_at_utc
FROM deduplicated_bronze
WHERE row_num = 1
  AND latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND velocity >= 0