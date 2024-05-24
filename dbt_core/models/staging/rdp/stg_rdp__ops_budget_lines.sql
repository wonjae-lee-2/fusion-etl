with

ops_budget_lines as (

    select
        ops_budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        scenario,
        cost_center,
        budget_category,
        output_statement_id,
        account,
        expenditure_organization_code,
        implementer,
        currency_code,
        lc_amount,
        usd_amount,
        strategy_validity_key

    from {{ ref('base_rdp__ops_budget_lines') }}

),

strategy_validities as (

    select strategy_validity_key

    from {{ ref('base_rdp__strategy_validities') }}

),

output_statement_validities as (

    select distinct output_statement_id

    from {{ ref('base_rdp__output_statement_validities') }}

    where is_excluded = 0

)

select
    ops_budget_lines.ops_budget_line_id,
    ops_budget_lines.budget_year,
    ops_budget_lines.abc_code,
    ops_budget_lines.strategy_code,
    ops_budget_lines.budget_version,
    ops_budget_lines.scenario,
    ops_budget_lines.cost_center,
    ops_budget_lines.budget_category,
    ops_budget_lines.output_statement_id,
    ops_budget_lines.account,
    ops_budget_lines.expenditure_organization_code,
    ops_budget_lines.implementer,
    ops_budget_lines.currency_code,
    ops_budget_lines.lc_amount,
    ops_budget_lines.usd_amount

from ops_budget_lines

inner join strategy_validities
    on
        ops_budget_lines.strategy_validity_key
        = strategy_validities.strategy_validity_key

inner join output_statement_validities
    on
        ops_budget_lines.output_statement_id
        = output_statement_validities.output_statement_id
