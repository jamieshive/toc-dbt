with mismatches as (

    select
        *
    from {{ ref('base_cms__sku_code_mismatches')}}
),

final as (

    select
        sku,
        prod_desc,
        cheese,
        sausage,
        roni,
        veg,
        spin,
        crustless,
        hrtchz,
        hrtsaus,
        heart,
        gfcheese,
        gfsausage,
        gfpepperoni,
        meat,
        lous,
        bbq,
        hotg,
        price

    from mismatches

)

select * from final