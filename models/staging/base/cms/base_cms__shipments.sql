{# Creates a base table of shipment data from Panoply #}

with source as (
        
    select * from {{ source('public','tastes_shipments') }} 
        
)
    
select * from source