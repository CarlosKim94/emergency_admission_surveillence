{{ config(materialized='table') }}

SELECT
    admission_date,
    ed_type,
    syndrome,
    COUNT(*) as total_admissions,
    -- Calculate a 7-day moving average to smooth out the noise
    AVG(COUNT(*)) OVER (
        PARTITION BY ed_type, syndrome 
        ORDER BY admission_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_avg
FROM {{ source('staging_data', 'stg_admissions') }}
GROUP BY 1, 2, 3