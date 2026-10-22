{{ config(materialized='table') }}

SELECT
    observation_time,
    city_name,
    temperature_c,
    apparent_temperature_c,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    wind_gusts_kmh,
    pressure_hpa,
    CASE WHEN temperature_c >= 40 THEN TRUE ELSE FALSE END AS is_heatwave_candidate,
    CASE WHEN precipitation_mm >= 10 THEN TRUE ELSE FALSE END AS is_heavy_rain
FROM {{ ref('stg_weather') }}