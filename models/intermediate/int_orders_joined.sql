{# Creates table of orders from staged order view and stage IMM order view #}

{# Adds on column that denotes whether an order was a paid order or not #}

{# Adds on columns of fiscal period info related to order dates #}

with 

orders as (
        
    select 
        * 
        
    from {{ ref('stg_cms__orders') }}
{#
    UNION ALL

    select
        *
    
    from {{ ref('stg_bigc__orders')}}
#}
),

imm_orders as (

    select 
        * 
        
    from {{ ref('stg_imm__media_attributed_orders') }}

),

order_sources as (

    select
        *
    
    from {{ ref('stg_cms__order_source_codes') }}

),

fiscal_calendar as (

    select
        *
    
    from {{ ref('utils_calendar__created') }}
),

final as (
        
    select
        orders.*,

        case
            when orders.media_id = imm_orders.media_id
                then 'Paid'
            else 'Organic'
        end as order_attribution,

        order_sources.order_source_name,
        order_sources.order_source_number as order_src,
        order_sources.revenue_group,
        order_sources.revenue_segment,
        fiscal_calendar.year as order_date_fiscal_year,
        fiscal_calendar.fiscal_period as order_date_fiscal_period,
        fiscal_calendar.week_of_period as order_date_fiscal_week,
        fiscal_calendar.day_of_week as order_date_fiscal_day,
        fiscal_calendar.year_period as order_date_fiscal_year_period,
        fiscal_calendar.year_period_week as order_date_fiscal_year_period_week,
        fiscal_calendar.year_period_week_day as order_date_fiscal_year_period_week_day

    from orders

    left join imm_orders
        on orders.media_id = imm_orders.media_id
    left join fiscal_calendar
        on orders.ordered_on = fiscal_calendar.date_day
    left join order_sources
        on orders.order_source_number = order_sources.order_source_number
    
)

select * from final 