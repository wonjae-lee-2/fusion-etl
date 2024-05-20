with

source as (

    select * from {{ source('erp', 'GL_SEG_VAL_HIER_CF') }}

)

select
    job_utc_timestamp,
    tree_code,
    tree_version_name,
    distance,
    dep25_pk1_value,
    dep25_pk2_value,
    dep26_pk1_value,
    dep26_pk2_value,
    dep27_pk1_value,
    dep27_pk2_value,
    dep28_pk1_value,
    dep28_pk2_value,
    dep29_pk1_value,
    dep29_pk2_value,
    dep30_pk1_value,
    dep30_pk2_value

from source
