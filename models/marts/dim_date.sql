{{ config(materialized='table') }}

with date_range as (
    select 
        dateadd(day, seq4(), '2025-01-01') as dt
    from table(generator(rowcount => 365 * 20))   -- 10 years forward
)

select
    to_number(to_char(dt, 'YYYYMMDD')) as date_key,
    dt as date,
    year(dt) as year,
    quarter(dt) as quarter,
    month(dt) as month,
    to_char(dt, 'Month') as month_name,
    day(dt) as day,
    dayofweekiso(dt) as day_of_week,       -- Monday = 1
    to_char(dt, 'Day') as day_name,
    weekofyear(dt) as week_of_year
from date_range
