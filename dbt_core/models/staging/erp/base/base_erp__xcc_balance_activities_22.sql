with

source as (

    select * from {{ source('erp', 'XCC_BALANCE_ACTIVITIES_22') }}

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
    budget_ccid as budget_segment_id,
    code_combination_id as gl_segment_id,
    source_line_id_1,
    source_line_id_2,
    source_line_id_3,
    liquidation_line_id_1,
    liquidation_line_id_2,
    liquidation_line_id_3,
    pjc_project_id,
    pjc_task_id,
    pjc_expenditure_type_id,
    pjc_organization_id,
    entered_currency,
    entered_amount,
    amount as usd_amount,
    case vendor_id when '-10016' then null else vendor_id end as vendor_id

from source
