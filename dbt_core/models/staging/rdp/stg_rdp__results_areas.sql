with

output_statements as (

    select * from {{ ref('base_rdp__output_statements') }}

),

outcome_statements as (

    select * from {{ ref('base_rdp__outcome_statements') }}

),

outcome_areas as (

    select * from {{ ref('base_rdp__outcome_areas') }}

),

impact_statements as (

    select * from {{ ref('base_rdp__impact_statements') }}

),

impact_areas as (

    select * from {{ ref('base_rdp__impact_areas') }}

)

select
    output_statements.output_statement_id,
    output_statements.output_statement,
    output_statements.output_statement_code,
    output_statements.output_statement_short_descr,
    output_statements.output_statement_long_descr,
    output_statements.pillar,
    output_statements.situation_code,
    outcome_statements.outcome_statement_id,
    outcome_statements.outcome_statement,
    outcome_statements.outcome_statement_code,
    outcome_statements.outcome_statement_short_descr,
    outcome_statements.outcome_statement_long_descr,
    outcome_areas.outcome_area_id,
    outcome_areas.outcome_area,
    outcome_areas.outcome_area_short_descr,
    outcome_areas.outcome_area_long_descr,
    impact_statements.impact_statement_id,
    impact_statements.impact_statement,
    impact_statements.impact_statement_code,
    impact_statements.impact_statement_short_descr,
    impact_statements.impact_statement_long_descr,
    impact_areas.impact_area_id,
    impact_areas.impact_area,
    impact_areas.impact_area_short_descr,
    impact_areas.impact_area_long_descr

from output_statements

inner join outcome_statements
    on
        output_statements.outcome_statement_id
        = outcome_statements.outcome_statement_id

inner join outcome_areas
    on outcome_statements.outcome_area_id = outcome_areas.outcome_area_id

inner join impact_statements
    on
        outcome_statements.impact_statement_id
        = impact_statements.impact_statement_id

inner join impact_areas
    on impact_statements.impact_area_id = impact_areas.impact_area_id
