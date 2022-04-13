with base as (

    select
        *
    
    from {{ source('public','sheets_imm_creative_raw_data-supersetimport_20220331_imm_creative_raw_data-supersetimport_20220331')}}
)

select * from base