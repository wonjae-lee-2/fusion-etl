with

source as (

    select
        timestamp_utc,
        tree_version_name,
        dep30_pk1_value as account_category_group_code,
        dep30_pk1_description as account_category_group,
        dep29_pk1_value as account_category_code,
        dep29_pk1_description as account_category,
        dep28_pk1_value as account_code,
        dep28_pk1_description as account,
        dep30_pk1_value
        + ' '
        + dep30_pk1_description as account_category_group_tag,
        dep29_pk1_value + ' ' + dep29_pk1_description as account_category_tag,
        dep28_pk1_value + ' ' + dep28_pk1_description as account_tag

    from {{ source('erp', 'gl_accounts') }}

)

select
    timestamp_utc,
    tree_version_name,
    account_category_group_code,
    account_category_group,
    account_category_group_tag,
    account_category_code,
    account_category,
    account_category_tag,
    account_code,
    account,
    account_tag

from source
