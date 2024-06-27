with

source as (

    select
        timestamp_utc,
        tree_version_name,
        dep30_pk1_value as budget_category_group_code,
        dep30_pk1_description as budget_category_group,
        dep29_pk1_value as budget_category_code,
        dep29_pk1_description as budget_category,
        case
            when
                dep30_pk1_value = dep30_pk1_description
                then dep30_pk1_value
            else dep30_pk1_value + ' ' + dep30_pk1_description
        end as budget_category_group_tag,
        dep29_pk1_value + ' ' + dep29_pk1_description as budget_category_tag

    from {{ source('erp', 'gl_budget_categories') }}

)

select
    timestamp_utc,
    tree_version_name,
    budget_category_group_code,
    budget_category_group,
    budget_category_group_tag,
    budget_category_code,
    budget_category,
    budget_category_tag

from source
