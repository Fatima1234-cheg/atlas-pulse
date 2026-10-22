{{ config(materialized='table') }}

SELECT
    city_name,
    DATE(observation_time) AS date,
    ROUND(AVG(temperature_c), 1)          AS temp_avg_c,
    ROUND(MAX(temperature_c), 1)          AS temp_max_c,
    ROUND(MIN(temperature_c), 1)          AS temp_min_c,
    ROUND(MAX(apparent_temperature_c), 1) AS feels_like_max_c,
    ROUND(SUM(precipitation_mm), 1)       AS precipitation_total_mm,
    ROUND(AVG(humidity_pct), 0)           AS humidity_avg_pct,
    ROUND(AVG(wind_speed_kmh), 1)         AS wind_speed_avg_kmh,
    ROUND(MAX(wind_gusts_kmh), 1)         AS wind_gust_max_kmh,
    COUNT(*)                              AS valid_hours_count
FROM {{ ref('fct_weather_hourly') }}
GROUP BY city_name, DATE(observation_time)