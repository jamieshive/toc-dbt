{# Creates a base view of BigC order data from Panoply #}

with base as (
        
        select * from {{ source('public','bigc_tastes_of_chicago_orders') }}
        
)
    
select * from base