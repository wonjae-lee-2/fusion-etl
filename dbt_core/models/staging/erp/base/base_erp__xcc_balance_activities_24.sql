with

source as (

    select * from {{ source('erp', 'XCC_BALANCE_ACTIVITIES_24') }}

)

select
    job_utc_timestamp,
    period_name,
    budget_date,
    creation_date as budgetary_control_validation_date,
    activity_type_code,
    balance_type_code,
    sub_balance_type_code as balance_subtype_code,
    transaction_number,
    transaction_type_code,
    transaction_subtype_code,
    source_action_code as transaction_action_code,
    destination_type_code,
    liquidation_trans_type_code as liquidation_transaction_type_code,
    je_source_code as gl_source_code,
    je_category_code as gl_category_code,
    transaction_source_code as ppm_source_code,
    document_type_code as ppm_document_code,
    budget_ccid as bc_segment_id,
    code_combination_id as gl_segment_id,
    pjc_project_id,
    pjc_task_id,
    pjc_expenditure_type_id,
    pjc_organization_id,
    entered_currency,
    entered_amount,
    amount as usd_amount,
    replace(
        coalesce(source_line_id_1, '')
        + '-'
        + coalesce(source_line_id_2, '')
        + '-'
        + coalesce(source_line_id_3, ''),
        ' ',
        ''
    ) as source_line_id,
    replace(
        coalesce(liquidation_line_id_1, '')
        + '-'
        + coalesce(liquidation_line_id_2, '')
        + '-'
        + coalesce(liquidation_line_id_3, ''),
        ' ',
        ''
    ) as liquidation_line_id,
    case vendor_id when '-10016' then null else vendor_id end as vendor_id
    -- There are exceptional vendors which do not exist in the supplier list. 
    -- expense (vendor id = -10016)
    -- one time suppliers (vendor_id = -999)
    -- receivables (vendor_id = -222)

from source
