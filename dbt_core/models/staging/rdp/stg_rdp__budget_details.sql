with

ops_budget_lines as (

    select
        ops_budget_line_id as budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        strategy_validity_key,
        scenario,
        fund_code,
        cost_center_code,
        budget_category_group_code,
        account_category_code,
        expenditure_organization,
        original_expenditure_organization,
        output_statement_id,
        supplier_number,
        partnership_agreement_scope_code,
        negotiation_status,
        currency,
        lc_amount,
        usd_amount

    from {{ ref('base_rdp__ops_budget_lines') }}

),

apportioned_budget_lines as (

    select
        apportioned_budget_line_id as budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        strategy_validity_key,
        scenario,
        fund_code,
        cost_center_code,
        budget_category_group_code,
        null as account_category_code,
        null as expenditure_organization,
        null as original_expenditure_organization,
        output_statement_id,
        null as supplier_number,
        null as partnership_agreement_scope_code,
        null as negotiation_status,
        null as currency,
        null as lc_amount,
        usd_amount

    from {{ ref('base_rdp__apportioned_budget_lines') }}
),

budget_lines as (

    select * from ops_budget_lines

    union all

    select * from apportioned_budget_lines

),

strategy_validities as (

    select strategy_validity_key

    from {{ ref('base_rdp__strategy_validities') }}

),

output_validities as (

    select distinct output_statement_id

    from {{ ref('base_rdp__output_statement_validities') }}

),

output_statements as (

    select
        output_statement_id,
        output_statement_short_code,
        output_statement_long_code,
        output_statement,
        output_statement_description,
        population_group,
        budget_situation_code,
        outcome_statement_id

    from {{ ref('base_rdp__output_statements') }}

),

outcome_statements as (

    select
        outcome_statement_id,
        outcome_statement_short_code,
        outcome_statement_long_code,
        outcome_statement,
        outcome_statement_description,
        outcome_area_id,
        impact_statement_id

    from {{ ref('base_rdp__outcome_statements') }}

),

outcome_areas as (

    select
        outcome_area_id,
        outcome_area_code,
        outcome_area,
        outcome_area_description

    from {{ ref('base_rdp__outcome_areas') }}

),

impact_statements as (

    select
        impact_statement_id,
        impact_statement_short_code,
        impact_statement_long_code,
        impact_statement,
        impact_statement_description,
        impact_area_id

    from {{ ref('base_rdp__impact_statements') }}

),

impact_areas as (

    select
        impact_area_id,
        impact_area_code,
        impact_area,
        impact_area_description

    from {{ ref('base_rdp__impact_areas') }}

)

select
    budget_lines.budget_line_id,
    budget_lines.budget_year,
    budget_lines.abc_code,
    budget_lines.strategy_code,
    budget_lines.budget_version,
    budget_lines.scenario,
    budget_lines.fund_code,
    budget_lines.cost_center_code,
    budget_lines.budget_category_group_code,
    budget_lines.account_category_code,
    budget_lines.expenditure_organization,
    budget_lines.original_expenditure_organization,
    impact_areas.impact_area_code,
    impact_areas.impact_area,
    impact_areas.impact_area_description,
    impact_statements.impact_statement_short_code,
    impact_statements.impact_statement_long_code,
    impact_statements.impact_statement,
    impact_statements.impact_statement_description,
    outcome_areas.outcome_area_code,
    outcome_areas.outcome_area,
    outcome_areas.outcome_area_description,
    outcome_statements.outcome_statement_short_code,
    outcome_statements.outcome_statement_long_code,
    outcome_statements.outcome_statement,
    outcome_statements.outcome_statement_description,
    output_statements.output_statement_short_code,
    output_statements.output_statement_long_code,
    output_statements.output_statement,
    output_statements.output_statement_description,
    output_statements.population_group,
    output_statements.budget_situation_code,
    budget_lines.supplier_number,
    budget_lines.partnership_agreement_scope_code,
    budget_lines.negotiation_status,
    budget_lines.currency,
    budget_lines.lc_amount,
    budget_lines.usd_amount

from budget_lines

inner join strategy_validities
    on
        budget_lines.strategy_validity_key
        = strategy_validities.strategy_validity_key

inner join output_validities
    on
        budget_lines.output_statement_id
        = output_validities.output_statement_id

left join output_statements
    on budget_lines.output_statement_id = output_statements.output_statement_id

left join outcome_statements
    on
        output_statements.outcome_statement_id
        = outcome_statements.outcome_statement_id

left join outcome_areas
    on outcome_statements.outcome_area_id = outcome_areas.outcome_area_id

left join impact_statements
    on
        outcome_statements.impact_statement_id
        = impact_statements.impact_statement_id

left join impact_areas
    on impact_statements.impact_area_id = impact_areas.impact_area_id
