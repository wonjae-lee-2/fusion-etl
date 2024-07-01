with

source as (

    select
        outputstatementvalidityguid as output_statement_validity_id,
        outputstatementguid as output_statement_id,
        budgetyear as budget_year,
        abccode as abc_code,
        strategycode as strategy_code,
        outputstatement as output_statement

    from {{ source('rdp', 'OutputStatementValidity') }}

    where isdeleted = 0 and isvalid = 1 and isexcluded = 0

)

select * from source
