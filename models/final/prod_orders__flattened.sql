{# Creates flattened order table for use in BI that joins order table and customer table #}

with 

orders as (
        
    select * from {{ ref('int_orders_joined')}}

),

customers as (

    select * from {{ ref('int_customers_grouped') }}
),

first_event_info as (

    select
        orders.customer_id,
        min(revenue_group) as first_event_group,
        min(revenue_segment) as first_event_segment
    
    from orders

    left join customers
        on orders.customer_id = customers.customer_id

    where first_event_on = ordered_on

    group by 1
),

new_addresses as (

    select
        orders.address_concat,

        min(ordered_on) as new_to_file

    from orders

    group by 1
),

final as (

    select 
    distinct orders.order_id,
    orders.media_id,
    orders.customer_id,
    orders.item_total,
    orders.shipping_total,
    orders.discount_total,
    orders.state_tax_total,
    orders.order_total,
    orders.billing_address1,
    orders.billing_address2,
    orders.billing_city,
    orders.billing_state,
    orders.billing_zip,
    orders.was_cancelled,
    orders.ordered_on,
    orders.order_source_number,
    orders.adcode,
    orders.order_attribution,
    orders.order_source_name,
    orders.order_src,
    orders.revenue_group,
    orders.revenue_segment,
    orders.order_date_fiscal_year,
    orders.order_date_fiscal_period,
    orders.order_date_fiscal_week,
    orders.order_date_fiscal_day,
    orders.order_date_fiscal_year_period,
    orders.order_date_fiscal_year_period_week,
    customers.first_name,
    customers.last_name,
    customers.phone,
    customers.email,
    customers.company,
    customers.number_of_orders,
    customers.cohort,
    customers.cohort_group,
    customers.first_event_on,
    customers.first_event_fiscal_period,
    customers.first_event_fiscal_year_period,
    customers.first_event_fiscal_year_period_week,
    customers.most_recent_event_on,
    case
        when orders.ordered_on = customers.first_event_on 
            then 'Y'
        else 'N'
    end as is_new_customer,
    {{ dbt_utils.datediff('first_event_on', 'ordered_on', 'year') }} as years_since_first_event_on,
    {{ dbt_utils.datediff('first_event_on', 'ordered_on', 'day') }} as days_since_first_event_on,
    first_event_info.first_event_group,
    first_event_info.first_event_segment

    from orders

    left join customers
        on orders.customer_id = customers.customer_id
    left join first_event_info
        on orders.customer_id = first_event_info.customer_id
    left join new_addresses
        on orders.address_concat = new_addresses.address_concat

)

select * from final