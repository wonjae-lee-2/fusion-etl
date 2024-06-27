with

source_23 as (

    {{ select_actual_costs('erp', 'ppm_actual_costs_23') }} -- noqa: TMP 

),

source_24 as (

    {{ select_actual_costs('erp', 'ppm_actual_costs_24') }} -- noqa: TMP

),

source as (

    select * from source_23

    union all

    select * from source_24

)

select
    timestamp_utc,
    transaction_number,
    transaction_line_number,
    project_accounting_period,
    project_accounting_date,
    expenditure_item_date,
    project_unit,
    business_unit,
    task_id,
    expenditure_type_id,
    expenditure_organization_id,
    doc_entry_id,
    original_transaction_reference,
    po_number,
    receipt_number,
    invoice_number,
    fund_code,
    cost_center_code,
    account_code,
    budget_category_code,
    interfund_code,
    vendor_id,
    employee_id,
    employee_name,
    employee_type,
    job_title,
    grade,
    position_id,
    permanent_temporary_flag,
    unrb_flag,
    expenditure_batch,
    expenditure_comment,
    unit_of_measure,
    quantity,
    transaction_currency,
    transaction_currency_amount,
    usd_amount

from source
