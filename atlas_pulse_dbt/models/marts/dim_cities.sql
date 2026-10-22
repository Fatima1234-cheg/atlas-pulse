{{ config(materialized='table') }}

SELECT DISTINCT
    city_name,
    CASE city_name
        WHEN 'casablanca' THEN 33.5731
        WHEN 'rabat'      THEN 34.0209
        WHEN 'marrakech'  THEN 31.6295
        WHEN 'tanger'     THEN 35.7595
        WHEN 'agadir'     THEN 30.4278
    END AS latitude,
    CASE city_name
        WHEN 'casablanca' THEN -7.5898
        WHEN 'rabat'      THEN -6.8416
        WHEN 'marrakech'  THEN -7.9811
        WHEN 'tanger'     THEN -5.8340
        WHEN 'agadir'     THEN -9.5981
    END AS longitude
FROM {{ ref('stg_weather') }}