with

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
    output_statements.output_statement_id,
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
    output_statements.budget_situation_code

from output_statements

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
