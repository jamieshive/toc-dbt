with media_attributed_orders as (

    select 
        *
    
    from {{ ref('stg_imm__media_attributed_orders')}}
),

orders as (

    select
        ordered_on,
        media_id,
        is_new_customer
    
    from {{ ref('prod_orders__flattened')}}
),

final as (

    select
        media_attributed_orders.*,
        orders.is_new_customer

    from media_attributed_orders

    left join orders
        on media_attributed_orders.media_id = orders.media_id
)

select * from final