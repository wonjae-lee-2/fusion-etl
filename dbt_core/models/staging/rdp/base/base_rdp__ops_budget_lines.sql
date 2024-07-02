with

source as (

    select
        opsbudgetlineguid as ops_budget_line_id,
        budgetyear as budget_year,
        abccode as abc_code,
        strategycode as strategy_code,
        version as budget_version,
        scenario,
        '110' as fund_code,
        costcenter as cost_center_code,
        'OPS' as budget_category_group_code,
        account as account_category_code,
        expenditureorganisationcode as original_expenditure_organization,
        outputstatementguid as output_statement_id,
        implementer as supplier_number,
        partnershipagreementscopecode as partnership_agreement_scope_code,
        negotiationstatusdescr as negotiation_status,
        currencycode as currency,
        lcamount as lc_amount,
        usdamount as usd_amount,
        concat(budgetyear, '-', abccode, '-', strategycode, '-', version)
            as strategy_validity_key,
        case expenditureorganisationcode
            when 'GRC-THE-THE-OSO-01' then 'GRC-THE-THE-OFO-01'
            when 'GRC-CHI-CHI-OFO-01' then 'GRC-CHI-CHI-OFU-01'
            when 'NA' then null
        end as expenditure_organization

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
    fund_code,
    cost_center_code,
    budget_category_group_code,
    account_category_code,
    expenditure_organization,
    original_expenditure_organization,
    output_statement_id,
    supplier_number,
    partnership_agreement_scope_code,
    negotiation_status,
    currency,
    lc_amount,
    usd_amount

from source
