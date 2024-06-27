{% macro select_actual_costs(source_name, table_name) %}

    select
        timestamp_utc,
        expenditure_item_id as transaction_number,
        line_num as transaction_line_number,
        prvdr_pa_period_name as project_accounting_period,
        prvdr_pa_date as project_accounting_date,
        expenditure_item_date,
        project_unit_name as project_unit,
        business_unit_name as business_unit,
        task_id,
        expenditure_type_id,
        expenditure_organization_id,
        doc_entry_id,
        orig_transaction_reference as original_transaction_reference,
        doc_ref_id2 as po_number,
        doc_ref_id3 as receipt_number,
        invoice_num as invoice_number,
        case vendor_id
            when '-10016' then null
            else vendor_id
        end as vendor_id,
        person_number as employee_id,
        display_name as employee_name,
        user_person_type as employee_type,
        job_name as job_title,
        grade_code as grade,
        position_code as position_id,
        permanent_temporary_flag,
        expenditure_item_attribute1 as unrb_flag,
        user_batch_name as expenditure_batch,
        expenditure_comment,
        unit_of_measure,
        quantity,
        denom_currency_code as transaction_currency,
        denom_raw_cost as transaction_currency_amount,
        projfunc_raw_cost as usd_amount,
        case
            when pa1_segment1 is null and pa2_segment1 is null and rcv_segment1 is null and inv_segment1 is null then null
            else coalesce(pa1_segment1, '') + coalesce(pa2_segment1, '') + coalesce(rcv_segment1, '') + coalesce(inv_segment1, '')
        end as fund_code,
        case
            when pa1_segment2 is null and pa2_segment2 is null and rcv_segment2 is null and inv_segment2 is null then null
            else coalesce(pa1_segment2, '') + coalesce(pa2_segment2, '') + coalesce(rcv_segment2, '') + coalesce(inv_segment2, '')
        end as cost_center_code,
        case
            when pa1_segment3 is null and pa2_segment3 is null and rcv_segment3 is null and inv_segment3 is null then null
            else coalesce(pa1_segment3, '') + coalesce(pa2_segment3, '') + coalesce(rcv_segment3, '') + coalesce(inv_segment3, '')
        end as account_code,
        case
            when pa1_segment4 is null and pa2_segment4 is null and rcv_segment4 is null and inv_segment4 is null then null
            else coalesce(pa1_segment4, '') + coalesce(pa2_segment4, '') + coalesce(rcv_segment4, '') + coalesce(inv_segment4, '')
        end as budget_category_code,
        case
            when pa1_segment5 is null and pa2_segment5 is null and rcv_segment5 is null and inv_segment5 is null then null
            else coalesce(pa1_segment5, '') + coalesce(pa2_segment5, '') + coalesce(rcv_segment5, '') + coalesce(inv_segment5, '')
        end as interfund_code

    from {{ source(source_name, table_name) }}

{% endmacro %}
