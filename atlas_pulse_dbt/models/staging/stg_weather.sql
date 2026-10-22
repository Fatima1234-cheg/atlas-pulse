{{ config(materialized='view') }}

SELECT
    time                     AS observation_time,
    city                     AS city_name,
    temperature_2m           AS temperature_c,
    apparent_temperature     AS apparent_temperature_c,
    relative_humidity_2m     AS humidity_pct,
    precipitation            AS precipitation_mm,
    wind_speed_10m           AS wind_speed_kmh,
    wind_gusts_10m           AS wind_gusts_kmh,
    pressure_msl             AS pressure_hpa
FROM `plasma-streamer-480323-h7.atlas_pulse_silver.weather_clean`
WHERE temperature_2m IS NOT NULL