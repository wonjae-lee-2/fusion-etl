with

cost_center_trees as (

    select
        job_utc_timestamp,
        tree_version_name,
        dep25_pk1_value,
        dep26_pk1_value,
        dep27_pk1_value,
        dep28_pk1_value,
        dep29_pk1_value,
        dep30_pk1_value

    from {{ ref('base_erp__gl_seg_val_hier_cf') }}

    where
        tree_code = 'COA_CC'
        and distance = 6
),

descriptions as (

    select
        value,
        description

    from {{ ref('base_erp__fnd_vs_values_b') }}

    where attribute_category = 'HCR_COA_COSTCENTER'
),

cost_center_tree_descriptions as (

    select
        cost_center_trees.job_utc_timestamp,
        cost_center_trees.tree_version_name,
        cost_center_trees.dep30_pk1_value as field_hq_code,
        cost_center_trees.dep29_pk1_value as region_code,
        cost_center_trees.dep28_pk1_value as subregion_code,
        cost_center_trees.dep27_pk1_value as operation_code,
        cost_center_trees.dep26_pk1_value as suboffice_code,
        cost_center_trees.dep25_pk1_value as cost_center_code,
        max(
            case
                when
                    cost_center_trees.dep30_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as field_hq,
        max(
            case
                when
                    cost_center_trees.dep29_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as region,
        max(
            case
                when
                    cost_center_trees.dep28_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as subregion,
        max(
            case
                when
                    cost_center_trees.dep27_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as operation,
        max(
            case
                when
                    cost_center_trees.dep26_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as suboffice,
        max(
            case
                when
                    cost_center_trees.dep25_pk1_value = descriptions.value
                    then descriptions.description
            end
        ) as cost_center

    from cost_center_trees

    inner join descriptions
        on
            cost_center_trees.dep30_pk1_value = descriptions.value
            or cost_center_trees.dep29_pk1_value = descriptions.value
            or cost_center_trees.dep28_pk1_value = descriptions.value
            or cost_center_trees.dep27_pk1_value = descriptions.value
            or cost_center_trees.dep26_pk1_value = descriptions.value
            or cost_center_trees.dep25_pk1_value = descriptions.value

    group by
        cost_center_trees.job_utc_timestamp,
        cost_center_trees.tree_version_name,
        cost_center_trees.dep30_pk1_value,
        cost_center_trees.dep29_pk1_value,
        cost_center_trees.dep28_pk1_value,
        cost_center_trees.dep27_pk1_value,
        cost_center_trees.dep26_pk1_value,
        cost_center_trees.dep25_pk1_value

)

select
    job_utc_timestamp,
    tree_version_name,
    field_hq_code,
    field_hq,
    region_code,
    region,
    subregion_code,
    subregion,
    operation_code,
    operation,
    suboffice_code,
    suboffice,
    cost_center_code,
    cost_center,
    case
        when
            substring(
                suboffice_code,
                charindex('_', suboffice_code) + 1,
                len(suboffice_code)
            )
            = 'OTS'
            then 'Field Operations'
        else field_hq
    end as field_hq_reporting,
    case
        when field_hq_code = 'PHQGLO'
            then
                substring(
                    suboffice_code,
                    charindex('_', suboffice_code) + 1,
                    len(suboffice_code)
                )
        else region
    end as region_reporting,
    cost_center_code + ' ' + cost_center as cost_center_tag

from cost_center_tree_descriptions;
