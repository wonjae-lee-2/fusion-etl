with

source as (

    select
        apportionedbudgetlineguid as apportioned_budget_line_id,
        budgetyear as budget_year,
        abccode as abc_code,
        strategycode as strategy_code,
        version as budget_version,
        scenario,
        '110' as fund_code,
        costcenter as cost_center_code,
        budgetcategory as budget_category_group_code,
        outputstatementguid as output_statement_id,
        usdamount as usd_amount,
        concat(budgetyear, '-', abccode, '-', strategycode, '-', version)
            as strategy_validity_key

    from {{ source('rdp', 'ApportionedBudgetLine') }}

    where isdeleted = 0 and budgetyear >= 2023

)

select
    apportioned_budget_line_id,
    budget_year,
    abc_code,
    strategy_code,
    budget_version,
    strategy_validity_key,
    scenario,
    fund_code,
    cost_center_code,
    budget_category_group_code,
    output_statement_id,
    usd_amount

from source
