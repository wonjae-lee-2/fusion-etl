with

source as (

    select * from {{ source('erp', 'PJF_PROJECTS_ALL_B') }}
)

select
    job_utc_timestamp,
    project_id,
    segment1 as project_number,
    name as project_name

from source
