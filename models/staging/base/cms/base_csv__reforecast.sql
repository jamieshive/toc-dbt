with source as (
        
    select * from {{ source('public','reforecast') }}
        
),

staged as (

    select 
    cohort,
    "group" as revenue_group,
    segment as revenue_segment,
    period,
    "forecast by period" as forecast_by_period,
    "forecast by week" as forecast_by_week,
    "forecast by day" as forecast_by_day

    from source
)

select * from staged