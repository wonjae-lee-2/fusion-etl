with

source as (

    select
        timestamp_utc,
        commitment_txn_id as transaction_number,
        pa_period as project_accounting_period,
        pa_date as project_accounting_date,
        expenditure_item_date,
        bu_name as business_unit,
        task_id,
        expenditure_type_id,
        organization_id as expenditure_organization_id,
        doc_entry_id,
        cmt_number as commitment_number,
        description,
        cmt_approved_status as approved_status,
        transaction_status_code as transaction_status,
        cmt_requestor_name as requestor_name,
        cmt_buyer_name as buyer_name,
        unit_of_measure,
        quantity_ordered as ordered_quantity,
        tot_cmt_quantity as open_quantity,
        denom_currency_code as transaction_currency,
        amount_ordered as transaction_currency_ordered_amount,
        denom_raw_cost as transaction_currency_open_amount,
        projfunc_raw_cost as usd_open_amount,
        case line_type
            when 'R' then 'Requisition'
            when 'P' then 'Purchase Order'
            when 'I' then 'Invoice'
            else 'Unknown'
        end as commitment_type,
        case line_type
            when 'R' then req_segment1
            when 'P' then po_segment1
            when 'I' then inv_segment1
        end as fund_code,
        case line_type
            when 'R' then req_segment2
            when 'P' then po_segment2
            when 'I' then inv_segment2
        end as cost_center_code,
        case line_type
            when 'R' then req_segment3
            when 'P' then po_segment3
            when 'I' then inv_segment3
        end as account_code,
        case line_type
            when 'R' then req_segment4
            when 'P' then po_segment4
            when 'I' then inv_segment4
        end as budget_category_code,
        case line_type
            when 'R' then req_segment5
            when 'P' then po_segment5
            when 'I' then inv_segment5
        end as interfund_code,
        case vendor_id
            when '-10016' then null
            else vendor_id
        end as vendor_id

    from {{ source('erp', 'ppm_committed_costs') }}
)

select
    timestamp_utc,
    transaction_number,
    project_accounting_period,
    project_accounting_date,
    expenditure_item_date,
    business_unit,
    task_id,
    expenditure_type_id,
    expenditure_organization_id,
    doc_entry_id,
    commitment_type,
    commitment_number,
    description,
    fund_code,
    cost_center_code,
    account_code,
    budget_category_code,
    interfund_code,
    approved_status,
    transaction_status,
    requestor_name,
    buyer_name,
    vendor_id,
    unit_of_measure,
    ordered_quantity,
    open_quantity,
    transaction_currency,
    transaction_currency_ordered_amount,
    transaction_currency_open_amount,
    usd_open_amount

from source
