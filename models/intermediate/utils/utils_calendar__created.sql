{# Generates a fiscal calendar #}

select 
    d_day as date_day, 
    day_of_week, 
    day_of_period,
    day_of_year,
    num_of_days_counter,
    week_of_period,
    fiscal_period,
    year, 
    year_period, 
    year_period_week,
    year_period_week_day
     
from 
    (select 
        {{ get_fiscal_calendar('date_day')}} 
    from ({{ dbt_utils.date_spine(datepart="day",start_date="cast('2010-12-30' as date)",end_date="cast('2028-01-01' as date)")}}))

