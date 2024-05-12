with

source as (

    select * from {{ source('rdp', 'OutcomeStatement') }}

)

select
    outcomestatementguid as outcome_statement_id,
    outcomestatement as outcome_statement,
    outcomestatementcode as outcome_statement_code,
    outcomestatementshortdescr as outcome_statement_short_descr,
    outcomestatementlongdescr as outcome_statement_long_descr,
    outcomeareaguid as outcome_area_id,
    impactstatementguid as impact_statement_id

from source

where isdeleted = 0
