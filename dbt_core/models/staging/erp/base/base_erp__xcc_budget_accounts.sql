with

source as (

    select * from {{ source('erp', 'XCC_BUDGET_ACCOUNTS' ) }}

)

select
    budget_code_combination_id as bc_segment_id,
    segment_value1 as bc_fund_code,
    segment_value2 as bc_cost_center_code,
    segment_value3 as bc_account_group_code,
    segment_value4 as bc_budget_category_group_code

from source

where budget_chart_of_accounts_id = '14001' -- OL Detail
