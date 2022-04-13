
{# Creates table of shipment data from staged shipment view #}

{# Adds on columns of fiscal period info related to shipment dates #}

with shipments as (
        
    select 
        * 
    
    from {{ ref('stg_cms__shipments') }}   
{#
    UNION ALL

    select
        *

    from {{ ref('stg_bigc__shipments')}}
#} 
),

fiscal_calendar as (

    select 
        *

    from {{ ref('utils_calendar__created') }}
),

final as (
        
    select
        shipments.*,
        fiscal_calendar.fiscal_period as shipped_date_fiscal_period,
        fiscal_calendar.year_period as shipped_date_fiscal_year_period,
        fiscal_calendar.year_period_week as shipped_date_fiscal_year_period_week
        
    from shipments

    left join fiscal_calendar
        on shipments.shipped_on = fiscal_calendar.date_day
    
)
    
select * from final