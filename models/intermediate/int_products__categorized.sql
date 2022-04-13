{% set pizza_type = 

{"C":"Cheese","S":"Sausage", "R":"Pepperoni","V":"Veggie", "H":"Spinach", "X":"Crustless", "Y":"Heart_Cheese", 
"Z":"Heart_Sausage","E":"GF_Cheese","A":"GF_Sausage","N":"GF_Pepperoni","M":"Meat","L":"Lous_Special","B":"BBQ_Chicken","I":"HotG"} %}

with base_products as (

    select
        *
    
    from {{ ref('stg_cms__products')}}
),

items as (

    select
        *

    from {{ ref('stg_cms__order_items')}}
),

other_products as (

    select
    distinct
        items.prod_code,
        items.prod_desc,
        items.price,
        0 as is_discontinu,
        base_products.sku

    from items

    left join base_products
        on items.prod_code = base_products.sku

    where base_products.sku is NULL
    and items.prod_desc is not NULL
),

products as (

    select
        *

    from base_products

    UNION

    select
        prod_code as sku,
        lower(prod_desc) as prod_desc,
        price,
        0 as exp_cost,
        'unknown' as brand,
        cast(cast(is_discontinu as int) as boolean) as is_discontinued

    from other_products
),

codes as (

    select
        *
    
    from {{ ref('stg_cms__product_code_translation')}}
),

combos as (

    select
        sku,
        prod_desc,

        is_discontinued,
        split_part(sku,'-',1) as parent_sku,
        case
            when
                len(parent_sku) = 4 and parent_sku != 'LOUS'
            then
                split_part(sku,'-',3)
            else split_part(sku,'-',2)
        end as child,
        price
    
    from products

    where charindex('-',sku) > 0 
    and len(child) = 3
    and substring(child,1,1) > 0 
    or (child = 'Pay'
    or child = 'cheese'
    or child = 'thelou'
    or child = 'roni'
    or child = 'crustless'
    or child = 'heartSsg'
    or child = 'GFcheese'
    or child = 'veggie'
    or child = 'sausage'
    or child = 'spinach'
    or child = 'GFsausage'
    or child = 'heartChz'
    or child = 'GFroni'
    )
    and parent_sku != 'VG'
),

noncombos as (

    select

    products.sku,
    products.prod_desc,
    products.is_discontinued,
    split_part(products.sku,'-',1) as parent_sku,
    null as child_sku,
    products.price
    
    from products

    left join combos
        on products.sku = combos.sku
        where combos.sku IS NULL

),

pizza_packs_non_cast as (

    select
        sku,
        prod_desc,
        is_discontinued,
        'LS' as parent_sku,
        substring(sku,3,len(sku)-2) as child,
        {% for abbr,long in pizza_type.items() %}
        case
            when
                charindex('{{ abbr }}',child) > 0
            then substring(child,charindex('{{ abbr }}',child)+1,1)
            else
                '0'
        end as {{ long }},
        {% endfor %}
        price
    
    from noncombos

    where substring(sku,1,2) = 'LS'
),

pizza_packs_alter as (

    select
        sku,
        prod_desc,
        is_discontinued,
        cheese,
        case
            when
                sku = 'LS7000Special'
            then 
                '0'
            else
                sausage
        end as sausage,
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
        hotg,
        price
    
    from pizza_packs_non_cast
),

pizza_packs as (

    select
        sku,
        prod_desc,
        is_discontinued,
        cast(cheese as int),
        cast(sausage as int),
        cast(pepperoni as int),
        cast(veggie as int),
        cast(spinach as int),
        cast(crustless as int),
        cast(heart_cheese as int),
        cast(heart_sausage as int),
        cast(gf_cheese as int),
        cast(gf_sausage as int),
        cast(gf_pepperoni as int),
        cast(meat as int),
        cast(lous_special as int),
        cast(bbq_chicken as int),
        cast(hotg as int),
        price

    from pizza_packs_alter
),

nonpizza as (

    select
        *
    
    from noncombos

    where substring(sku,1,2) != 'LS'

),

combos_coded as (
    select 
        combos.*,
        codes.cheese,
        codes.sausage,
        codes.pepperoni as roni,
        codes.veggie as veg,
        codes.spinach as spin,
        codes.crustless,
        codes.heart_cheese as hrtchz,
        codes.heart_sausage as hrtsaus,
        case
            when
                hrtchz = 1 or hrtsaus = 1
            then
                1
            else
                0
        end as heart,
        codes.gf_cheese as gfcheese,
        codes.gf_sausage as gfsausage,
        codes.gf_pepperoni as gfpepperoni,
        codes.meat,
        codes.lous_special as lous,
        codes.bbq_chicken as bbq,
        codes.hotg

    from combos

    left join codes 
        on codes.child = combos.child

),

{% set pizza_type_list = ["cheese","sausage", "roni","veg", "spin", "crustless", "heart", "hrtchz", 
"hrtsaus","gfcheese","gfsausage","gfpepperoni","meat","lous","bbq","hotg"] %}

prod_desc_combos as (
    select 
        sku,
        {% for pizza in pizza_type_list %}
        case
            when
                charindex('{{ pizza }}',prod_desc) > 0
            then 
                '1'
            else
                '0'
        end as contains_{{ pizza }},
        {% endfor %}
        prod_desc
    from combos
),

comparisons as (

    select
        combos_coded.sku,
        combos_coded.prod_desc,
        {% for pizza in pizza_type_list %}
        case
            when
                prod_desc_combos.contains_{{ pizza }} = combos_coded.{{ pizza }}
            then
                combos_coded.{{ pizza }}::varchar
            else
                ( 'desc' || prod_desc_combos.contains_{{ pizza }} || 'code' || combos_coded.{{ pizza }} )
        end as {{ pizza }},
        {% endfor %}
        combos_coded.price,
        is_discontinued,
        parent_sku,
        child
    
    from combos_coded

    left join prod_desc_combos
        on prod_desc_combos.sku = combos_coded.sku

),

{% set pizza_type_l = ["sausage", "roni","veg", "spin", "crustless", "heart", "hrtchz", 
"hrtsaus","gfcheese","gfsausage","gfpepperoni","meat","lous","bbq","hotg"] %}

mismatches as (

    select
        *

    from comparisons

    where (substring(cheese,5,1) = 0
    or substring(cheese,10,1) = 0
    {% for pizza in pizza_type_l%}
    or substring({{ pizza }},5,1) = 0
    or substring({{ pizza }},10,1) = 0
    {% endfor %})
),

matches as (

    select
        combos_coded.*

    from combos_coded

    left join mismatches 
        on mismatches.sku = combos_coded.sku

    where mismatches.sku IS NULL
),

misses as (

    select
        *
    
    from {{ ref('stg_cms__sku_code_mismatches')}}
),

mismatches_coded as (
    
    select
        misses.sku,
        misses.prod_desc,
        mismatches.is_discontinued,
        mismatches.parent_sku,
        mismatches.child,
        misses.price,
        misses.cheese,
        misses.sausage,
        misses.roni,
        misses.veg,
        misses.spin,
        misses.crustless,
        misses.hrtchz,
        misses.hrtsaus,
        misses.heart,
        misses.gfcheese,
        misses.gfsausage,
        misses.gfpepperoni,
        misses.meat,
        misses.lous,
        misses.bbq,
        misses.hotg

    from misses

    left join mismatches
        on mismatches.sku = misses.sku
        and mismatches.prod_desc = misses.prod_desc
),

combos_unioned as (

    select
        sku,
        prod_desc,
        cheese,
        sausage,
        roni as pepperoni,
        veg as veggie,
        spin as spinach,
        crustless,
        hrtchz as heart_cheese,
        hrtsaus as heart_sausage,
        gfcheese as gf_cheese,
        gfsausage as gf_sausage,
        gfpepperoni as gf_pepperoni,
        meat,
        lous,
        bbq,
        hotg,
        is_discontinued

    from mismatches_coded

    UNION

    select
        sku,
        prod_desc,
        isnull(cheese,0),
        isnull(sausage,0),
        isnull(roni,0) as pepperoni,
        isnull(veg,0) as veggie,
        isnull(spin,0) as spinach,
        isnull(crustless,0),
        isnull(hrtchz,0) as heart_cheese,
        isnull(hrtsaus,0) as heart_sausage,
        isnull(gfcheese,0) as gf_cheese,
        isnull(gfsausage,0) as gf_sausage,
        isnull(gfpepperoni,0) as gf_pepperoni,
        isnull(meat,0),
        isnull(lous,0),
        isnull(bbq,0),
        isnull(hotg,0),
        is_discontinued


    from matches

),

all_products as (

    select
        sku,
        prod_desc,
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
        lous_special as lous,
        bbq_chicken as bbq,
        hotg,
        is_discontinued,
        'pizza pack' as product_category

    from pizza_packs

    UNION

    select
        *,
        'pizza combo' as product_category

    from combos_unioned 

    UNION

    select
        sku,
        prod_desc,
        cast(0 as int) as cheese,
        cast(0 as int) as sausage,
        cast(0 as int) as pepperoni,
        cast(0 as int) as veggie,
        cast(0 as int) as spinach,
        cast(0 as int) as crustless,
        cast(0 as int) as heart_cheese,
        cast(0 as int) as heart_sausage,
        cast(0 as int) as gf_cheese,
        cast(0 as int) as gf_sausage,
        cast(0 as int) as gf_pepperoni,
        cast(0 as int) as meat,
        cast(0 as int) as lous,
        cast(0 as int) as bbq,
        cast(0 as int) as hotg,
        is_discontinued,
        'non-pizza' as product_category

    from nonpizza

),

final as (

    select
        all_products.*,
        products.exp_cost,
        lower(products.brand) as brand

    from all_products

    left join products
        on products.sku = all_products.sku
)

select * from final
