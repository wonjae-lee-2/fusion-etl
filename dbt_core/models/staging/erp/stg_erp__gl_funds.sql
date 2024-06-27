with

source as (

    select
        timestamp_utc,
        tree_version_name,
        dep30_pk1_value as fund_category_code,
        dep30_pk1_description as fund_category,
        dep29_pk1_value as fund_code,
        dep29_pk1_description as fund,
        dep30_pk1_value + ' ' + dep30_pk1_description as fund_category_tag,
        dep29_pk1_value + ' ' + dep29_pk1_description as fund_tag

    from {{ source('erp', 'gl_funds') }}

)

select
    timestamp_utc,
    tree_version_name,
    fund_category_code,
    fund_category,
    fund_category_tag,
    fund_code,
    fund,
    fund_tag

from source
