with

source as (

    select * from {{ source('erp', 'XCC_BUDGET_ACCOUNTS' ) }}

)

select
    budget_code_combination_id as budget_segment_id,
    segment_value1 as budget_segment1,
    segment_value2 as budget_segment2,
    segment_value3 as budget_segment3,
    segment_value4 as budget_segment4

from source

where budget_chart_of_accounts_id = '14001'
