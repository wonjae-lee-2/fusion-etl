with

source as (

    select * from {{ source('erp', 'GL_CODE_COMBINATIONS' ) }}

)

select
    job_utc_timestamp,
    code_combination_id as gl_segment_id,
    segment1 as gl_segment1,
    segment2 as gl_segment2,
    segment3 as gl_segment3,
    segment4 as gl_segment4,
    segment5 as gl_segment5

from source
