with

budget_lines as (

    select
        ops_budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        strategy_validity_key,
        scenario,
        cost_center_code,
        budget_category_group_code,
        account_code,
        expenditure_organization,
        output_statement_id,
        implementer_code,
        partnership_agreement_scope_code,
        negotiation_status,
        currency,
        lc_amount,
        usd_amount

    from {{ ref('base_rdp__ops_budget_lines') }}

),

strategy_validities as (

    select strategy_validity_key

    from {{ ref('base_rdp__strategy_validities') }}

),

output_validities as (

    select distinct output_statement_id

    from {{ ref('base_rdp__output_statement_validities') }}

)

select
    budget_lines.ops_budget_line_id,
    budget_lines.budget_year,
    budget_lines.abc_code,
    budget_lines.strategy_code,
    budget_lines.budget_version,
    budget_lines.scenario,
    budget_lines.cost_center_code,
    budget_lines.budget_category_group_code,
    budget_lines.account_code,
    budget_lines.expenditure_organization,
    budget_lines.output_statement_id,
    budget_lines.implementer_code,
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
