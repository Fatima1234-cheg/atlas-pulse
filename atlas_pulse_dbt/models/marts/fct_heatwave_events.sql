{{ config(materialized='table') }}

WITH heatwave_days AS (
    SELECT city_name, date, temp_max_c
    FROM {{ ref('agg_city_daily') }}
    WHERE temp_max_c >= 35
),
islands AS (
    SELECT *,
        DATE_SUB(date, INTERVAL ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY date) DAY) AS grp
    FROM heatwave_days
)
SELECT
    city_name,
    MIN(date)                 AS start_date,
    MAX(date)                 AS end_date,
    COUNT(*)                  AS duration_days,
    ROUND(MAX(temp_max_c), 1) AS max_temperature_c,
    35.0                      AS threshold_c
FROM islands
GROUP BY city_name, grp
HAVING COUNT(*) >= 3