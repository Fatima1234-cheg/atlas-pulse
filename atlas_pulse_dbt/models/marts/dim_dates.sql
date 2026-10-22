{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT DATE(observation_time) AS full_date
    FROM {{ ref('stg_weather') }}
)
SELECT
    full_date,
    EXTRACT(YEAR FROM full_date)  AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    FORMAT_DATE('%B', full_date)  AS month_name,
    EXTRACT(DAY FROM full_date)   AS day,
    CASE
        WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Hiver'
        WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5)  THEN 'Printemps'
        WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8)  THEN 'Été'
        ELSE 'Automne'
    END AS season
FROM dates