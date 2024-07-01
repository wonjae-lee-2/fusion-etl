with

budget_lines as (

    select
        apportioned_budget_line_id,
        budget_year,
        abc_code,
        strategy_code,
        budget_version,
        strategy_validity_key,
        scenario,
        cost_center_code,
        budget_category_group_code,
        output_statement_id,
        usd_amount

    from {{ ref('base_rdp__apportioned_budget_lines') }}
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
    budget_lines.apportioned_budget_line_id,
    budget_lines.budget_year,
    budget_lines.abc_code,
    budget_lines.strategy_code,
    budget_lines.budget_version,
    budget_lines.scenario,
    budget_lines.cost_center_code,
    budget_lines.budget_category_group_code,
    budget_lines.output_statement_id,
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
