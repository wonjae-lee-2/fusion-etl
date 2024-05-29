with

source as (

    select * from {{ source('erp', 'PJF_PROJ_ELEMENTS_B') }}
)

select
    job_utc_timestamp,
    proj_element_id as task_id,
    project_id,
    element_number as task_number,
    name as task_name,
    description as task_description

from source
