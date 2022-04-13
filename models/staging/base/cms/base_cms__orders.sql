{# Creates a base view of order data from Panoply #}

with source as (
        
    select 
    
        id as order_id,
        reforderid as media_id,
        cust_id as customer_id,
        item_total,
        shipping as shipping_total,
        discount_amount as discount_total,
        state_tax as state_tax_total,
        order_total,
        address1 as billing_address1,
        address2 as billing_address2,
        city as billing_city,
        state as billing_state,
        zip as billing_zip,
        order_src,
        adcode,
        CAST((billing_address1 || billing_city || billing_state || billing_zip ) as varchar) as address_concat,
        cancelled as was_cancelled,
        CAST(order_date as date) as ordered_on
    
    from {{ source('public','tastes_orders') }}
        
)
    
select * from source