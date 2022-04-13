with items as (

    select
        *
    
    from {{ ref('base_cms__order_items')}}
),

final as (

    select
        order_id,
        prod_code,
        packagenumber,
        prod_desc,
        price,
        fulfill_date,
        sent_qty

    from items
)

select * from final 