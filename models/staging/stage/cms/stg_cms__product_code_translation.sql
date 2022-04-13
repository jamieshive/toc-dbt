with codes as (

    select
        *

    from {{ ref('base_cms__product_code_translation')}}
),

final as (

    select
        code as child,
        cheese,
        sausage,
        pepperoni,
        veggie,
        spinach,
        crustless,
        heart_cheese,
        heart_sausage,
        gf_cheese,
        gf_sausage,
        gf_pepperoni,
        meat,
        lous_special,
        bbq_chicken,
        hotg

    from codes

)

select * from final