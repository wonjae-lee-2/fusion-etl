with

source as (

    select
        outputstatementguid as output_statement_id,
        outputstatement as output_statement_short_code,
        outputstatementcode as output_statement_long_code,
        outputstatementshortdescr as output_statement,
        outputstatementlongdescr as output_statement_description,
        pillar as population_group,
        situationcode as budget_situation_code,
        outcomestatementguid as outcome_statement_id

    from {{ source('rdp', 'OutputStatement') }}

    where isdeleted = 0


)

select * from source
