{# Creates flattened shipments table for use in BI that joins the shipment table and customer table #}

with

shipments as (

    select * from {{ ref('int_shipments_filtered') }}
),

customers as (

    select * from {{ ref('int_customers_grouped') }}
),

orders as (

    select * from {{ ref('int_orders_joined')}}

),

shipments_joined as (

    select 
    shipments.shipping_id,
    shipments.order_id,
    shipments.media_id,
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
    shipments.shipped_date_fiscal_year_period_week,
    orders.order_total,
    customers.customer_id,
    customers.email,
    customers.first_name,
    customers.last_name,
    customers.phone,
    customers.company,
    customers.number_of_orders,
    customers.cohort,
    customers.first_event_on,
    customers.first_event_fiscal_period,
    customers.first_event_fiscal_year_period,
    customers.first_event_fiscal_year_period_week,
    customers.most_recent_event_on,
    case
        when orders.ordered_on = customers.first_event_on 
            then 'Y'
        else 'N'
    end as is_new_customer

    from shipments

    left join orders
        on shipments.order_id = orders.order_id
    left join customers
        on orders.customer_id = customers.customer_id

),

weighted_revenue as (

    select

        order_id,
        count(order_id) as count,
        avg(order_total)/count(order_id) as revenue

        from shipments_joined

        group by order_id

),

final as (

    select 
        shipments_joined.shipping_id,
        shipments_joined.order_id,
        shipments_joined.customer_id,
        shipments_joined.email,
        shipments_joined.media_id,
        shipments_joined.address_id,
        shipments_joined.shipping_address1,
        shipments_joined.shipping_address2,
        shipments_joined.shipping_city,
        shipments_joined.shipping_state,
        shipments_joined.shipping_zip,
        shipments_joined.sel_letter,
        shipments_joined.track_num,
        shipments_joined.was_gifted,
        shipments_joined.shipped_on,
        shipments_joined.shipped_date_fiscal_period,
        shipments_joined.shipped_date_fiscal_year_period,
        shipments_joined.shipped_date_fiscal_year_period_week,
        shipments_joined.first_name,
        shipments_joined.last_name,
        shipments_joined.phone,
        shipments_joined.company,
        shipments_joined.cohort,
        shipments_joined.first_event_on,
        shipments_joined.first_event_fiscal_period,
        shipments_joined.first_event_fiscal_year_period,
        shipments_joined.first_event_fiscal_year_period_week,
        shipments_joined.most_recent_event_on,
        shipments_joined.is_new_customer,
        weighted_revenue.revenue as order_total

    from shipments_joined

    left join weighted_revenue
        on shipments_joined.order_id = weighted_revenue.order_id

)

select * from final
