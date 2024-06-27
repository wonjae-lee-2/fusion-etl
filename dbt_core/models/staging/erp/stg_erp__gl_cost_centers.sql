with

source as (

    select
        timestamp_utc,
        tree_version_name,
        dep30_pk1_value as field_hq_code,
        dep30_pk1_description as field_hq,
        dep29_pk1_value as region_code,
        dep29_pk1_description as region,
        dep28_pk1_value as subregion_code,
        dep28_pk1_description as subregion,
        dep27_pk1_value as operation_code,
        dep27_pk1_description as operation,
        dep26_pk1_value as suboffice_code,
        dep26_pk1_description as suboffice,
        dep25_pk1_value as cost_center_code,
        dep25_pk1_description as cost_center,
        case
            when
                substring(
                    dep26_pk1_value,
                    charindex('_', dep26_pk1_value) + 1,
                    len(dep26_pk1_value)
                )
                = 'OTS'
                then 'Field Operations'
            else dep30_pk1_description
        end as field_hq_reporting,
        case
            when dep30_pk1_value = 'PHQGLO'
                then
                    substring(
                        dep26_pk1_value,
                        charindex('_', dep26_pk1_value) + 1,
                        len(dep26_pk1_value)
                    )
            else dep29_pk1_description
        end as region_reporting,
        dep25_pk1_value + ' ' + dep25_pk1_description as cost_center_tag


    from {{ source('erp', 'gl_cost_centers') }}

)

select
    timestamp_utc,
    tree_version_name,
    field_hq_code,
    field_hq,
    field_hq_reporting,
    region_code,
    region,
    region_reporting,
    subregion_code,
    subregion,
    operation_code,
    operation,
    suboffice_code,
    suboffice,
    cost_center_code,
    cost_center,
    cost_center_tag

from source
