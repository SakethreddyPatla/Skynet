{{
    config(
        materialized='table',
        file_format='delta',
        pre_hook="""
        COPY INTO skynet.default.flight_ops_bronze
        FROM '/Volumes/skynet/default/raw_open_sky_landing/'
        FILEFORMAT = JSON;
    """
    )
}}

with raw_bronze as (
    select * from {{ source('skynet_source', 'flight_ops_bronze') }}
)

select
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
FROM raw_bronze
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND velocity >= 0