with

source as (

    select
        opsbudgetlineguid as ops_budget_line_id,
        budgetyear as budget_year,
        abccode as abc_code,
        strategycode as strategy_code,
        version as budget_version,
        scenario,
        costcenter as cost_center_code,
        'OPS' as budget_category_group_code,
        account as account_code,
        expenditureorganisationcode as expenditure_organization,
        outputstatementguid as output_statement_id,
        implementer as implementer_code,
        partnershipagreementscopecode as partnership_agreement_scope_code,
        negotiationstatusdescr as negotiation_status,
        currencycode as currency,
        lcamount as lc_amount,
        usdamount as usd_amount,
        concat(budgetyear, '-', abccode, '-', strategycode, '-', version)
            as strategy_validity_key

    from {{ source('rdp', 'OPSBudgetLine') }}

    where isdeleted = 0 and budgetyear >= 2023

)

select
    ops_budget_line_id,
    budget_year,
    abc_code,
    strategy_code,
    budget_version,
    strategy_validity_key,
    scenario,
    cost_center_code,
    budget_category_group_code,
    account_code,
    expenditure_organization,
    output_statement_id,
    implementer_code,
    partnership_agreement_scope_code,
    negotiation_status,
    currency,
    lc_amount,
    usd_amount

from source
