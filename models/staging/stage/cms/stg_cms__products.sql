with products as (

    select
        *
    
    from {{ ref('base_cms__products')}}
),

final as (

    select
        prod_code as sku,
        lower(prod_desc) as prod_desc,
        price,
        exp_cost,
        prodrepgde as brand,
        discontinu as is_discontinued

    from products
)

select * from final