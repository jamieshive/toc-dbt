{# Creates a base view of BigC order shipment data from Panoply #}

with shipments as (
        
        select * from {{ source('public','bigc_tastes_of_chicago_order_shipments') }}
        
), 

casts as (

        select 
                shipments.id,
                shipments.order_id as order_id,
                shipments.customer_id as customer_id,
                shipments.tracking_number as track_num,
                cast(shipments.date_created AS date) as shipped_on

        from shipments
)
    
select * from casts