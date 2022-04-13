{# Creates a base view of BigC shipping address data from Panoply #}

with shipping_addresses as (
        
        select * from {{ source('public','bigc_tastes_of_chicago_order_shipments_shipping_address') }}
        
),

casts as (

        select
                cast(shipping_addresses.bigc_tastes_of_chicago_order_shipments_id AS varchar) as bigc_tastes_of_chicago_order_shipments_id,
                shipping_addresses.email,
                shipping_addresses.street_1 as shipping_address1,
                shipping_addresses.street_2 as shipping_address2,
                shipping_addresses.city as shipping_city,
                shipping_addresses.state as shipping_state,
                cast(shipping_addresses.zip AS varchar) as shipping_zip
        
    from shipping_addresses
)
        
    
select * from casts