{# Creates table of customer data from staged customer view and staged order view #}

{# Adds on three columns that calculate the first event date, number of orders, and most recent event date of a customer #}

{# Adds on column that calculates the Cohort of a customer based on their first event date #}

{# Adds on columns of fiscal period info related to first event dates #}

with customers as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        company

    from {{ ref('stg_cms__customers') }}
{#
    UNION ALL

    select
        cast(customer_id AS varchar) as customer_id,
        first_name,
        last_name,
        email,
        phone,
        company
    
    from {{ ref('stg_bigc__customers')}}
#}
),

orders as (

    select
        order_id,
        customer_id,
        ordered_on

    from {{ ref('stg_cms__orders') }}
{#
    UNION ALL

    select 
        cast(order_id AS varchar) as order_id,
        cast(customer_id AS varchar) as customer_id,
        ordered_on
    
    from {{ ref('stg_bigc__orders')}}
#}
),

customer_orders as (

    select
        customer_id, {# NOTE: change to email when everyone is ready #}

        min(ordered_on) as first_event_on,
        max(ordered_on) as most_recent_event_on,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

fiscal_calendar as (

    select
        *
    
    from {{ ref('utils_calendar__created') }}
),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.phone,
        customers.company,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.first_event_on,
        fiscal_calendar.year as cohort,
            case 
        when cohort = '2011' 
        or cohort = '2012' 
        or cohort = '2013' 
        or cohort = '2014' 
        or cohort = '2015' 
        or cohort = '2016'
        or cohort = '2017'
        or cohort = '2018'
        or cohort = '2019'
            then 'Legacy'
        when cohort = '2020'
            then '2020'
        when cohort = '2021'
            then '2021'
        when cohort = '2022'
            then '2022'
        else 'unknown'
    end as cohort_group,
        fiscal_calendar.fiscal_period as first_event_fiscal_period,
        fiscal_calendar.year_period as first_event_fiscal_year_period,
        fiscal_calendar.year_period_week as first_event_fiscal_year_period_week,
        customer_orders.most_recent_event_on

    from customers

    left join customer_orders 
        on customers.customer_id = customer_orders.customer_id
    left join fiscal_calendar
        on customer_orders.first_event_on = fiscal_calendar.date_day

    where first_event_on is NOT NULL
)

select * from final