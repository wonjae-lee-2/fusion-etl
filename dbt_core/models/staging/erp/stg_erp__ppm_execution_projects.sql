with

source as (

    select
        timestamp_utc,
        proj_element_id as task_id,
        project_segment1 as project_number,
        project_name,
        project_unit_name as project_unit,
        business_unit_name as business_unit,
        project_type,
        project_attribute1 as budget_year,
        project_attribute2 as fund_code,
        project_attribute3 as original_budget_category_group_code,
        element_number as task_number,
        task_name,
        task_attribute1 as impact_area,
        task_attribute2 as impact_statement,
        task_attribute3 as outcome_area,
        task_attribute4 as outcome_statement,
        task_attribute5 as population_group,
        task_attribute6 as sdg,
        task_attribute7 as budget_situation_code,
        task_attribute8 as marker,
        case project_attribute3
            when 'OS' then 'OPS'
            when 'AO' then 'ABOD'
            when 'CM' then 'CMF'
            when '00' then '000'
            else project_attribute3
        end as budget_category_group_code

    from {{ source('erp', 'ppm_projects') }}

    where project_unit_name = 'Execution Project Unit'

)

select
    timestamp_utc,
    task_id,
    project_number,
    project_name,
    project_unit,
    business_unit,
    project_type,
    budget_year,
    fund_code,
    budget_category_group_code,
    original_budget_category_group_code,
    task_number,
    task_name,
    impact_area,
    impact_statement,
    outcome_area,
    outcome_statement,
    population_group,
    sdg,
    budget_situation_code,
    marker

from source
