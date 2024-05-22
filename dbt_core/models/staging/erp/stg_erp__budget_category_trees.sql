with

budget_category_trees as (

    select
        job_utc_timestamp,
        tree_version_name,
        dep29_pk1_value,
        dep30_pk1_value

    from {{ ref('base_erp__gl_seg_val_hier_cf') }}

    where
        tree_code = 'COA_BCAT'
        and distance = 2
),

descriptions as (

    select
        value,
        description

    from {{ ref('base_erp__fnd_vs_values_b') }}

    where attribute_category = 'HCR_COA_BUDGETCATEGORY'
),

budget_category_tree_descriptions as (

    select
        budget_category_trees.job_utc_timestamp,
        budget_category_trees.tree_version_name,
        budget_category_trees.dep30_pk1_value as budget_category_group_code,
        budget_category_trees.dep29_pk1_value as budget_category_code,
        max(
            case
                when
                    budget_category_trees.dep30_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as budget_category_group,
        max(
            case
                when
                    budget_category_trees.dep29_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as budget_category

    from budget_category_trees

    inner join descriptions
        on
            budget_category_trees.dep30_pk1_value = descriptions.value
            or budget_category_trees.dep29_pk1_value = descriptions.value

    group by
        budget_category_trees.job_utc_timestamp,
        budget_category_trees.tree_version_name,
        budget_category_trees.dep30_pk1_value,
        budget_category_trees.dep29_pk1_value

)

select
    job_utc_timestamp,
    tree_version_name,
    budget_category_group_code,
    budget_category_group,
    budget_category_code,
    budget_category,
    budget_category_group_code
    + ' '
    + budget_category_group as budget_category_group_tag,
    budget_category_code + ' ' + budget_category as budget_category_tag

from budget_category_tree_descriptions;
