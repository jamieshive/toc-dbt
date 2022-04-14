with ads as (

    select
        *

    from {{ ref('microsoft_ads__ad_adapter')}}
)

select * from ads