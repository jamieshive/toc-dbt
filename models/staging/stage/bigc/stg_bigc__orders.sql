{# Takes base BigC orders view, renames columns, and removes unnecessary columns #}

with source as (
        
    select * from {{ ref('base_bigc__orders') }}
        
),
    
casts as (
        
    select
        id as order_id,
        NULL as media_id,
        cast(customer_id AS varchar) as customer_id,
        items_total as item_total,
        shipping_cost_inc_tax as shipping_total,
        discount_amount as discount_total,
        cast(NULL AS bigint) as state_tax_total,
        total_inc_tax as order_total,

        case 
            when
                order_source = 'www'
            then '57'
            when
                order_source = 'manual'
            then '1'

        end as order_source_number,
        NULL as billing_address1,
        NULL as billing_address2,
        NULL as billing_city,
        NULL as billing_state,
        NULL as billing_zip,
        NULL as address_concat,
        NULL as adcode,

        case 
            when 
                status = 'Cancelled'
                then '0'
            else '1'
        end as was_cancelled,

        CAST(date_created as date) as ordered_on
        
    from source
    
), 

staged as (
        
    select

        order_id,
        media_id,
        customer_id,
        item_total,
        shipping_total,
        discount_total,
        state_tax_total,
        order_total,
        billing_address1,
        billing_address2,
        billing_city,
        billing_state,
        billing_zip,
        address_concat,
        CAST(was_cancelled as varchar),
        CAST(ordered_on as date) as ordered_on,
        CAST(order_source_number as varchar),
        adcode
        
    from casts
    
)
    
select * from staged