with

lookups as (

    select
        lookup_type,
        lookup_code,
        meaning

    from {{ ref('base_erp__fnd_lookup_values_tl') }}

),

ppm_sources as (

    select
        source_code,
        source_name

    from {{ ref('base_erp__pjf_txn_sources_b') }}

),

ppm_documents as (

    select
        document_code,
        document_name

    from {{ ref('base_erp__pjf_txn_document_b') }}

),

bc_segments as (

    select
        bc_segment_id,
        bc_fund_code,
        bc_cost_center_code,
        bc_account_group_code,
        bc_budget_category_group_code

    from {{ ref('base_erp__xcc_budget_accounts') }}

),

gl_segments as (

    select
        gl_segment_id,
        gl_fund_code,
        gl_cost_center_code,
        gl_account_code,
        gl_budget_category_code,
        gl_interfund_code

    from {{ ref('base_erp__gl_code_combinations') }}

),

suppliers as (

    select
        vendor_id,
        supplier_number,
        supplier_name,
        supplier_type,
        supplier_business_classification

    from {{ ref('base_erp__poz_suppliers') }}

),

projects as (

    select
        project_id,
        project_number,
        project_name

    from {{ ref('base_erp__pjf_projects_all_b') }}

),

tasks as (

    select
        task_id,
        task_number,
        task_name

    from {{ ref('base_erp__pjf_proj_elements_b') }}

),

expenditure_types as (

    select
        expenditure_type_id,
        expenditure_category_id,
        expenditure_type_code,
        expenditure_type

    from {{ ref('base_erp__pjf_exp_types_b') }}

),

expenditure_categories as (

    select
        expenditure_category_id,
        expenditure_category_code,
        expenditure_category

    from {{ ref('base_erp__pjf_exp_categories_tl') }}

),

expenditure_categories_types as (

    select
        expenditure_types.expenditure_type_id,
        expenditure_types.expenditure_type_code,
        expenditure_types.expenditure_type,
        expenditure_categories.expenditure_category_code,
        expenditure_categories.expenditure_category

    from expenditure_types

    left join expenditure_categories
        on
            expenditure_types.expenditure_category_id
            = expenditure_categories.expenditure_category_id

),

expenditure_organizations as (

    select
        organization_id,
        organization_name as expenditure_organization,
        organization_description as expenditure_organization_description

    from {{ ref('base_erp__hr_all_organization_units_f') }}

),

activities_22 as (

    select
        job_utc_timestamp,
        period_name,
        budget_date,
        budgetary_control_validation_date,
        activity_type_code,
        balance_type_code,
        balance_subtype_code,
        transaction_number,
        transaction_type_code,
        transaction_subtype_code,
        transaction_action_code,
        destination_type_code,
        liquidation_transaction_type_code,
        gl_source_code,
        gl_category_code,
        ppm_source_code,
        ppm_document_code,
        bc_segment_id,
        gl_segment_id,
        vendor_id,
        pjc_project_id,
        pjc_task_id,
        pjc_expenditure_type_id,
        pjc_organization_id,
        source_line_id,
        liquidation_line_id,
        entered_currency,
        entered_amount,
        usd_amount

    from {{ ref('base_erp__xcc_balance_activities_22') }}
),

activities_23 as (

    select
        job_utc_timestamp,
        period_name,
        budget_date,
        budgetary_control_validation_date,
        activity_type_code,
        balance_type_code,
        balance_subtype_code,
        transaction_number,
        transaction_type_code,
        transaction_subtype_code,
        transaction_action_code,
        destination_type_code,
        liquidation_transaction_type_code,
        gl_source_code,
        gl_category_code,
        ppm_source_code,
        ppm_document_code,
        bc_segment_id,
        gl_segment_id,
        vendor_id,
        pjc_project_id,
        pjc_task_id,
        pjc_expenditure_type_id,
        pjc_organization_id,
        source_line_id,
        liquidation_line_id,
        entered_currency,
        entered_amount,
        usd_amount

    from {{ ref('base_erp__xcc_balance_activities_23') }}

),

activities_24 as (

    select
        job_utc_timestamp,
        period_name,
        budget_date,
        budgetary_control_validation_date,
        activity_type_code,
        balance_type_code,
        balance_subtype_code,
        transaction_number,
        transaction_type_code,
        transaction_subtype_code,
        transaction_action_code,
        destination_type_code,
        liquidation_transaction_type_code,
        gl_source_code,
        gl_category_code,
        ppm_source_code,
        ppm_document_code,
        bc_segment_id,
        gl_segment_id,
        vendor_id,
        pjc_project_id,
        pjc_task_id,
        pjc_expenditure_type_id,
        pjc_organization_id,
        source_line_id,
        liquidation_line_id,
        entered_currency,
        entered_amount,
        usd_amount

    from {{ ref('base_erp__xcc_balance_activities_24') }}

),

activities as (

    select * from activities_22
    union all
    select * from activities_23
    union all
    select * from activities_24

)

select
    activities.job_utc_timestamp,
    activities.period_name,
    activities.budget_date,
    activities.budgetary_control_validation_date,
    lookups_activity_type.meaning as activity_type,
    lookups_balance_type.meaning as balance_type,
    lookups_balance_subtype.meaning as balance_subtype,
    activities.transaction_number,
    lookups_transaction_type.meaning as transaction_type,
    lookups_transaction_subtype.meaning as transaction_subtype,
    lookups_transaction_action.meaning as transaction_action,
    lookups_destination_type.meaning as destination_type,
    lookups_liquidation_transaction_type.meaning
        as liquidation_transaction_type,
    activities.gl_source_code,
    activities.gl_category_code,
    ppm_sources.source_name as ppm_source,
    ppm_documents.document_name as ppm_document,
    bc_segments.bc_fund_code,
    bc_segments.bc_cost_center_code,
    bc_segments.bc_account_group_code,
    bc_segments.bc_budget_category_group_code,
    gl_segments.gl_fund_code,
    gl_segments.gl_cost_center_code,
    gl_segments.gl_account_code,
    gl_segments.gl_budget_category_code,
    gl_segments.gl_interfund_code,
    suppliers.supplier_number,
    suppliers.supplier_name,
    suppliers.supplier_type,
    suppliers.supplier_business_classification,
    projects.project_number,
    projects.project_name,
    tasks.task_number,
    tasks.task_name,
    expenditure_categories_types.expenditure_category_code,
    expenditure_categories_types.expenditure_category,
    expenditure_categories_types.expenditure_type_code,
    expenditure_categories_types.expenditure_type,
    expenditure_organizations.expenditure_organization,
    expenditure_organizations.expenditure_organization_description,
    activities.source_line_id,
    activities.liquidation_line_id,
    activities.entered_currency,
    activities.entered_amount,
    activities.usd_amount

from activities

left join lookups as lookups_activity_type
    on
        activities.activity_type_code = lookups_activity_type.lookup_code
        and lookups_activity_type.lookup_type = 'XCC_BALANCE_ACTIVITY_TYPES'

left join lookups as lookups_balance_type
    on
        activities.balance_type_code = lookups_balance_type.lookup_code
        and lookups_balance_type.lookup_type = 'XCC_FUNDS_AVAIL_BAL_BUCKETS'

left join lookups as lookups_balance_subtype
    on
        activities.balance_subtype_code = lookups_balance_subtype.lookup_code
        and lookups_balance_subtype.lookup_type = 'XCC_RESERV_CATEGORY_BUCKETS'

left join lookups as lookups_transaction_type
    on
        activities.transaction_type_code = lookups_transaction_type.lookup_code
        and lookups_transaction_type.lookup_type = 'XCC_TRANSACTION_TYPES'

left join lookups as lookups_transaction_subtype
    on
        activities.transaction_subtype_code
        = lookups_transaction_subtype.lookup_code
        and lookups_transaction_subtype.lookup_type = 'XCC_TRANSACTION_SUBTYPES'

left join lookups as lookups_transaction_action
    on
        activities.transaction_action_code
        = lookups_transaction_action.lookup_code
        and lookups_transaction_action.lookup_type
        = 'XCC_TRANSACTION_TYPE_ACTION'

left join lookups as lookups_destination_type
    on
        activities.destination_type_code = lookups_destination_type.lookup_code
        and lookups_destination_type.lookup_type = 'XCC_RECVING_DEST_TYPE'

left join lookups as lookups_liquidation_transaction_type
    on
        activities.liquidation_transaction_type_code
        = lookups_liquidation_transaction_type.lookup_code
        and lookups_liquidation_transaction_type.lookup_type
        = 'XCC_TRANSACTION_TYPES'

left join ppm_sources
    on activities.ppm_source_code = ppm_sources.source_code

left join ppm_documents
    on activities.ppm_document_code = ppm_documents.document_code

left join bc_segments
    on activities.bc_segment_id = bc_segments.bc_segment_id

left join gl_segments
    on activities.gl_segment_id = gl_segments.gl_segment_id

left join suppliers
    on activities.vendor_id = suppliers.vendor_id

left join projects
    on activities.pjc_project_id = projects.project_id

left join tasks
    on activities.pjc_task_id = tasks.task_id

left join expenditure_categories_types
    on
        activities.pjc_expenditure_type_id
        = expenditure_categories_types.expenditure_type_id

left join expenditure_organizations
    on
        activities.pjc_organization_id
        = expenditure_organizations.organization_id
