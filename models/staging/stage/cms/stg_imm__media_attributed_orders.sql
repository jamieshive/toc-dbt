{# Takes base IMM orders view, renames columns, and removes unnecessary columns #}

with source as (
        
    select * from {{ ref('base_imm__media_attributed_orders') }}
        
),
    
staged as (
        
    select
        id as imm_id,
        cast(conversion_ts as date) as ordered_on,
        media_attributed_order_id as media_id,
        placement_name,
        product_cost,
        site,
        banner_name
        
    from source
    
)
    
select * from staged