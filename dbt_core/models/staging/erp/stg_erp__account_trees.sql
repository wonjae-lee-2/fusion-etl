with

account_trees as (

    select
        job_utc_timestamp,
        tree_version_name,
        dep28_pk1_value,
        dep29_pk1_value,
        dep30_pk1_value

    from {{ ref('base_erp__gl_seg_val_hier_cf') }}

    where
        tree_code = 'COA_BC'
        and distance = 3
),

descriptions as (

    select
        value,
        description

    from {{ ref('base_erp__fnd_vs_values_b') }}

    where attribute_category = 'HCR_COA_ACCOUNT'
),

account_tree_descriptions as (

    select
        account_trees.job_utc_timestamp,
        account_trees.tree_version_name,
        account_trees.dep30_pk1_value as account_summary_code,
        account_trees.dep29_pk1_value as account_group_code,
        account_trees.dep28_pk1_value as account_code,
        max(
            case
                when
                    account_trees.dep30_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as account_summary,
        max(
            case
                when
                    account_trees.dep29_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as account_group,
        max(
            case
                when
                    account_trees.dep28_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as account

    from account_trees

    inner join descriptions
        on
            account_trees.dep30_pk1_value = descriptions.value
            or account_trees.dep29_pk1_value = descriptions.value
            or account_trees.dep28_pk1_value = descriptions.value

    group by
        account_trees.job_utc_timestamp,
        account_trees.tree_version_name,
        account_trees.dep30_pk1_value,
        account_trees.dep29_pk1_value,
        account_trees.dep28_pk1_value

)

select
    job_utc_timestamp,
    tree_version_name,
    account_summary_code,
    account_summary,
    account_group_code,
    account_group,
    account_code,
    account,
    account_summary_code + ' ' + account_summary as account_summary_tag,
    account_group_code + ' ' + account_group as account_group_tag,
    account_code + ' ' + account as account_tag

from account_tree_descriptions;
