with

apportioned_budget_lines as (

    select
        apportioned_budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        scenario,
        cost_center,
        budget_category,
        output_statement_id,
        usd_amount,
        strategy_validity_key

    from {{ ref('base_rdp__apportioned_budget_lines') }}
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
    apportioned_budget_lines.apportioned_budget_line_id,
    apportioned_budget_lines.budget_year,
    apportioned_budget_lines.abc_code,
    apportioned_budget_lines.strategy_code,
    apportioned_budget_lines.budget_version,
    apportioned_budget_lines.scenario,
    apportioned_budget_lines.cost_center,
    apportioned_budget_lines.budget_category,
    apportioned_budget_lines.output_statement_id,
    apportioned_budget_lines.usd_amount

from apportioned_budget_lines

inner join strategy_validities
    on
        apportioned_budget_lines.strategy_validity_key
        = strategy_validities.strategy_validity_key

inner join output_statement_validities
    on
        apportioned_budget_lines.output_statement_id
        = output_statement_validities.output_statement_id
