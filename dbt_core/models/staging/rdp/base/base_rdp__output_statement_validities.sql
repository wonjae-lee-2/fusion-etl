with

source as (

    select * from {{ source('rdp', 'OutputStatementValidity') }}

)

select
    outputstatementvalidityguid as output_statement_validity_id,
    budgetyear as budget_year,
    abccode as abc_code,
    strategycode as strategy_code,
    outputstatement as output_statement,
    outputstatementguid as output_statement_id,
    isexcluded as is_excluded

from source

where isvalid = 1 and isdeleted = 0
