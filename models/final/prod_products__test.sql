{{ config(materialized='ephemeral') }}

with skus as (

    select
        *

    from {{ ref('int_products__categorized')}}
),

orders as (
        
    select * from {{ ref('prod_orders__flattened')}}

),

order_items as (

    select
        *

    from {{ ref('stg_cms__order_items')}}

),

order_by_product as (

    select
        order_items.order_id,
        skus.*,
        order_items.fulfill_date,
        (order_items.price * order_items.sent_qty) as pre_total,
        exp_cost

    from order_items

    left join skus
        on skus.sku = order_items.prod_code
),

flattened as (

    select
        order_id,
        sum(pre_total) as sum_total,
        count(order_id) as num_brands

    from order_by_product

    group by 1
),

orders_flattened as (

    select
        order_by_product.*,
        order_by_product.pre_total::float + ((((order_by_product.pre_total::float - 
            (order_by_product.pre_total::float - orders.order_total::float)))::float 
            - flattened.sum_total)::float/flattened.num_brands)::float as order_total,
        orders.revenue_group,
        orders.revenue_segment,
        orders.ordered_on,
        orders.cohort_group,
        orders.media_id

    from order_by_product

    left join flattened
        on order_by_product.order_id = flattened.order_id

    left join orders
        on orders.order_id = order_by_product.order_id
)

select * from orders_flattened