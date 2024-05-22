with

fund_trees as (

    select
        job_utc_timestamp,
        tree_version_name,
        dep29_pk1_value,
        dep30_pk1_value

    from {{ ref('base_erp__gl_seg_val_hier_cf') }}

    where
        tree_code = 'COA_FUND'
        and distance = 2
),

descriptions as (

    select
        value,
        description

    from {{ ref('base_erp__fnd_vs_values_b') }}

    where attribute_category = 'HCR_COA_FUND'
),

fund_tree_descriptions as (

    select
        fund_trees.job_utc_timestamp,
        fund_trees.tree_version_name,
        fund_trees.dep30_pk1_value as fund_group_code,
        fund_trees.dep29_pk1_value as fund_code,
        max(
            case
                when
                    fund_trees.dep30_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as fund_group,
        max(
            case
                when
                    fund_trees.dep29_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as fund

    from fund_trees

    inner join descriptions
        on
            fund_trees.dep30_pk1_value = descriptions.value
            or fund_trees.dep29_pk1_value = descriptions.value

    group by
        fund_trees.job_utc_timestamp,
        fund_trees.tree_version_name,
        fund_trees.dep30_pk1_value,
        fund_trees.dep29_pk1_value

)

select
    job_utc_timestamp,
    tree_version_name,
    fund_group_code,
    fund_group,
    fund_code,
    fund,
    fund_group_code + ' ' + fund_group as fund_group_tag,
    fund_code + ' ' + fund as fund_tag

from fund_tree_descriptions;
