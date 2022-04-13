{# Takes base customer view, renames columns, and removes unnecessary columns #}

with source as (
        
    select * from {{ ref('base_cms__customers') }}
        
),
    
staged as (
        
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        phone,
        company
        
    from source
    
)
    
select * from staged
