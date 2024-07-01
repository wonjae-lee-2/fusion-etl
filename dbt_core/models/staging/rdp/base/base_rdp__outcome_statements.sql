with

source as (

    select
        outcomestatementguid as outcome_statement_id,
        outcomestatement as outcome_statement_short_code,
        outcomestatementcode as outcome_statement_long_code,
        outcomestatementshortdescr as outcome_statement,
        outcomestatementlongdescr as outcome_statement_description,
        outcomeareaguid as outcome_area_id,
        impactstatementguid as impact_statement_id

    from {{ source('rdp', 'OutcomeStatement') }}

    where isdeleted = 0

)

select * from source
