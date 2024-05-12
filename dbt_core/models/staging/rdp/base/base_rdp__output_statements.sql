with

source as (

    select * from {{ source('rdp', 'OutputStatement') }}

)

select
    outputstatementguid as output_statement_id,
    outputstatement as output_statement,
    outputstatementcode as output_statement_code,
    outputstatementshortdescr as output_statement_short_descr,
    outputstatementlongdescr as output_statement_long_descr,
    pillar,
    outcomestatementguid as outcome_statement_id,
    concat(pillar, situationcode) as situation_code

from source

where isdeleted = 0
