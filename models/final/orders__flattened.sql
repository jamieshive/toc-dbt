{# Creates flattened order table for use in BI that joins order table, shipment table, and customer table #}

with 

orders as (
        
    select * from {{ ref('int_orders_joined')}}

),

customers as (

    select * from {{ ref('int_customers_grouped') }}
),

shipments as (

    select * from {{ ref('int_shipments_filtered') }}
),

orders_and_customers_joined as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.phone,
        customers.company,
        customers.number_of_orders,
        customers.first_event_on,
        customers.cohort,
        customers.first_event_fiscal_period,
        customers.first_event_fiscal_year_period,
        customers.first_event_fiscal_year_period_week,
        customers.most_recent_event_on,
        orders.order_id,
        orders.media_id,
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
        orders.order_source_name,
        case
            when
                email = 'vanessarobinson@goldbelly.com'
            then '105'
            else order_source_number
        end as order_src,
        orders.adcode,
        orders.order_attribution,
        orders.order_date_fiscal_year,
        orders.order_date_fiscal_period,
        orders.order_date_fiscal_week,
        orders.order_date_fiscal_day,
        orders.order_date_fiscal_year_period,
        orders.order_date_fiscal_year_period_week,
        orders.order_date_fiscal_year_period_week_day,
        case
            when orders.ordered_on = customers.first_event_on 
                then 'Y'
            else 'N'
        end as is_new_customer

    from orders
    
    left join customers
        on customers.customer_id = orders.customer_id
),

final as (

    select 
        orders_and_customers_joined.order_id,
        orders_and_customers_joined.media_id,
        orders_and_customers_joined.customer_id,
        orders_and_customers_joined.item_total,
        orders_and_customers_joined.shipping_total,
        orders_and_customers_joined.discount_total,
        orders_and_customers_joined.state_tax_total,
        orders_and_customers_joined.order_total,
        orders_and_customers_joined.billing_address1,
        orders_and_customers_joined.billing_address2,
        orders_and_customers_joined.billing_city,
        orders_and_customers_joined.billing_state,
        orders_and_customers_joined.billing_zip,
        orders_and_customers_joined.was_cancelled,
        orders_and_customers_joined.ordered_on,
        orders_and_customers_joined.adcode,
        orders_and_customers_joined.order_src as ordersrc,
        orders_and_customers_joined.order_source_name,
        orders_and_customers_joined.order_attribution,
        orders_and_customers_joined.order_date_fiscal_year_period,
        orders_and_customers_joined.order_date_fiscal_year_period_week,
        orders_and_customers_joined.order_date_fiscal_period,
        orders_and_customers_joined.is_new_customer,
        orders_and_customers_joined.first_name,
        orders_and_customers_joined.last_name,
        orders_and_customers_joined.email,
        orders_and_customers_joined.phone,
        orders_and_customers_joined.company,
        orders_and_customers_joined.number_of_orders,
        orders_and_customers_joined.first_event_on,
        orders_and_customers_joined.cohort,
        orders_and_customers_joined.first_event_fiscal_period,
        orders_and_customers_joined.first_event_fiscal_year_period,
        orders_and_customers_joined.first_event_fiscal_year_period_week,
        orders_and_customers_joined.most_recent_event_on,
        shipments.shipping_id,
        shipments.address_id,
        shipments.shipping_address1,
        shipments.shipping_address2,
        shipments.shipping_city,
        shipments.shipping_state,
        shipments.shipping_zip,
        shipments.sel_letter,
        shipments.track_num,
        shipments.was_gifted,
        shipments.shipped_on,
        shipments.shipped_date_fiscal_period,
        shipments.shipped_date_fiscal_year_period,
        shipments.shipped_date_fiscal_year_period_week

    from orders_and_customers_joined

    left join shipments
        on orders_and_customers_joined.order_id = shipments.order_id

)

select * from final