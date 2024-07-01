with

source as (

    select
        strategyvalidityguid as strategy_validity_id,
        budgetyear as budget_year,
        abccode as abc_code,
        strategycode as strategy_code,
        lastbudgetversion as last_budget_version,
        lastbudgetversiondescr as last_budget_version_descr,
        concat(
            budgetyear, '-', abccode, '-', strategycode, '-', lastbudgetversion
        ) as strategy_validity_key

    from {{ source('rdp', 'StrategyValidity') }}

    where isdeleted = 0 and isvalid = 1

)

select * from source
