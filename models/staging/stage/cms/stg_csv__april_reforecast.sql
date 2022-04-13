with new_forecast as (

    select
        *

    from {{ ref('base_csv__april_reforecast')}}
),

stage as (

    select
        cohort,
        "group" as revenue_group,
        case
            when
                segment = 'Call Center'
            then
                'CallCenter'
            else
                segment
        end as revenue_segment,
        case
            when
                fiscal_period = 'P1'
            then
                'P01'
            when
                fiscal_period = 'P2'
            then 
                'P02'
            when
                fiscal_period = 'P3'
            then
                'P03'
            when
                fiscal_period = 'P4'
            then
                'P04'
            when
                fiscal_period = 'P5'
            then 
                'P05'
            when
                fiscal_period = 'P6'
            then
                'P06'
            when
                fiscal_period = 'P7'
            then
                'P07'
            when
                fiscal_period = 'P8'
            then 
                'P08'
            when
                fiscal_period = 'P9'
            then
                'P09'
            else
                fiscal_period
        end as order_date_fiscal_period,
        forecast_by_period

    from new_forecast
)

select * from stage order by 4