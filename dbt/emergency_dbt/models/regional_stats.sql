{{ config(materialized='table') }}

with raw_data as (
    select * from {{ source('staging_data', 'stg_admissions') }}
)

select 
    ed_type,                  -- This replaces 'region'
    syndrome,                -- Added this for more detail
    count(*) as total_records,
    avg(ed_count) as avg_daily_cases, 
    max(admission_date) as last_updated
from raw_data
group by 1, 2