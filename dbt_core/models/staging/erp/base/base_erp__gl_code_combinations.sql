with

source as (

    select * from {{ source('erp', 'GL_CODE_COMBINATIONS' ) }}

)

select
    job_utc_timestamp,
    code_combination_id as gl_segment_id,
    segment1 as gl_fund_code,
    segment2 as gl_cost_center_code,
    segment3 as gl_account_code,
    segment4 as gl_budget_category_code,
    segment5 as gl_interfund_code

from source
