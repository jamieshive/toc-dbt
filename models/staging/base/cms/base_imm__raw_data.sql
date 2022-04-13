with base as (

    select
        *

    from {{ source('public','sheets_imm_raw_data_imm-media_superset_20220331_imm_raw_data_imm-media_superset_20220331')}}
)

select * from base 