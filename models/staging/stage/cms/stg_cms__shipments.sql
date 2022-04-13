{# Takes base shipments view, renames columns, and removes unnecessary columns #}

{# Filters by shipment date to ensure that only shipment dates after 1900 are reported #}

with source as (
        
    select * from {{ ref('base_cms__shipments') }}
        
),
    
staged as (
        
    select
        cast(id AS varchar) as shipping_id,
        order_id,
        NULL::varchar as customer_id,
        NULL as email,
        reforderid as media_id,
        cast(address_id AS varchar) as address_id,
        address1 as shipping_address1,
        address2 as shipping_address2,
        city as shipping_city,
        state as shipping_state,
        zip as shipping_zip,
        sel_letter,
        track_num,
        gifted as was_gifted,
        cast(ship_date AS date) as shipped_on
        
    from source

    where shipped_on > '1900-01-01'  
    
)
    
select * from staged 