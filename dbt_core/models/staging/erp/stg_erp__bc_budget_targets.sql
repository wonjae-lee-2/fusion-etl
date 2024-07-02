with

source as (

    select
        timestamp_utc,
        header_id,
        line_number,
        creation_date as budget_entry_date,
        period_name as budget_period,
        source_budget as control_budget,
        segment1 as fund_code,
        segment2 as cost_center_code,
        segment4 as original_budget_category_group_code,
        attribute_char5 as goal,
        attribute_char4 as population_group,
        attribute_char3 as original_budget_situation_code,
        budget_action,
        attribute_char1 as budget_classification_type,
        attribute_char2 as budget_change_type,
        attribute_char7 as reference,
        data_set as budget_entry_name,
        annotation_txt as comment,
        amount as usd_amount,
        case segment4
            when 'PG' then 'STAFF'
            when 'PS' then 'STAFF'
            when 'AO' then 'ABOD'
            else segment4
        end as budget_category_group_code,
        case attribute_char3
            when '1900' then '900'
            else attribute_char3
        end as budget_situation_code

    from {{ source('erp', 'bc_budget_targets') }}

)

select
    timestamp_utc,
    header_id,
    line_number,
    budget_entry_date,
    budget_period,
    control_budget,
    fund_code,
    cost_center_code,
    budget_category_group_code,
    original_budget_category_group_code,
    goal,
    population_group,
    budget_situation_code,
    original_budget_situation_code,
    budget_action,
    budget_classification_type,
    budget_change_type,
    reference,
    budget_entry_name,
    comment,
    usd_amount

from source
