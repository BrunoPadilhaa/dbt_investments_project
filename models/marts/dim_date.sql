with date_range as (
    select 
        dateadd(day, seq4(), date '2025-01-01') as dt
    from table(generator(rowcount => 365 * 20))
)

select
    to_number(to_varchar(dt, 'YYYYMMDD'))              as date_id,
    dt                                                 as date,
    year(dt)                                           as year,
    quarter(dt)                                        as quarter_number,
    month(dt)                                          as month_number,
    to_varchar(dt, 'MMMM')                             as month_name,
    to_varchar(dt, 'MON')                              as month_abrv,
    to_varchar(dt, 'MON-YY')                           as month_abrv_year,
    day(dt)                                            as day,
    dayofweekiso(dt)                                   as day_of_week,  -- Monday = 1
    dayname(dt)                                        as day_name,
    weekofyear(dt)                                     as week_of_year
from date_range
