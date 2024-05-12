with

source as (

    select * from {{ source('rdp', 'OPSBudgetLine') }}

)

select
    opsbudgetlineguid as ops_budget_line_id,
    budgetyear as budget_year,
    abccode as abc_code,
    strategycode as strategy_code,
    version as budget_version,
    scenario,
    costcenter as cost_center,
    'OPS' as budget_category,
    outputstatementguid as output_statement_id,
    account,
    expenditureorganisationcode as expenditure_organization_code,
    implementer,
    currencycode as currency_code,
    lcamount as lc_amount,
    usdamount as usd_amount,
    concat(budgetyear, '-', abccode, '-', strategycode, '-', version)
        as strategy_validity_key

from source

where isdeleted = 0 and budgetyear >= 2023
