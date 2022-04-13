{# Takes base shipments view, renames columns, and removes unnecessary columns #}

{# Filters by shipment date to ensure that only shipment dates after 1900 are reported #}

with shipments as (
        
    select 
        id,
        cast(order_id AS varchar) as order_id,
        cast(customer_id AS varchar) as customer_id,
        track_num,
        cast(shipped_on AS date) as shipped_on 
    
    from {{ ref('base_bigc__order_shipments') }}
        
),

shipping_addresses as (

    select * from {{ ref('base_bigc__order_shipments_shipping_address')}}
),
    
staged as (
        
    select
        shipments.id as shipping_id,
        cast(shipments.order_id AS varchar) as order_id,
        cast(shipments.customer_id AS varchar) as customer_id,
        shipping_addresses.email,
        NULL as media_id,
        NULL as address_id,
        shipping_addresses.shipping_address1,
        shipping_addresses.shipping_address2,
        shipping_addresses.shipping_city,
        shipping_addresses.shipping_state,
        cast(shipping_addresses.shipping_zip AS varchar) as shipping_zip,
        NULL as sel_letter,
        shipments.track_num,
        cast(NULL as bigint) as was_gifted,
        cast(shipments.shipped_on AS date) as shipped_on
        
    from shipments

    left join shipping_addresses
        on shipments.id = shipping_addresses.bigc_tastes_of_chicago_order_shipments_id
    
)
    
select * from staged