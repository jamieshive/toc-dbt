{# Creates a base view of BigC billing address data from Panoply #}

with source as (
        
        select * from {{ source('public','bigc_tastes_of_chicago_order_shipments_billing_address') }}
        
)
    
select * from source