with

ops_budget_lines as (

    select * from {{ ref('base_rdp__ops_budget_lines') }}
),

strategy_validities as (

    select * from {{ ref('base_rdp__strategy_validities') }}
),

output_statements as (

    select distinct output_statement_id

    from {{ ref('base_rdp__output_statements') }}

),

output_statement_validities as (

    select distinct output_statement_id

    from {{ ref('base_rdp__output_statement_validities') }}

    where is_excluded = 1

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

inner join output_statements
    on
        ops_budget_lines.output_statement_id
        = output_statements.output_statement_id

left join output_statement_validities
    on
        ops_budget_lines.output_statement_id
        = output_statement_validities.output_statement_id

where
    output_statement_validities.output_statement_id is null
