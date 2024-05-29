with

source as (

    select * from {{ source('erp', 'HR_ALL_ORGANIZATION_UNITS_F') }}
)

select
    job_utc_timestamp,
    organization_id,
    name as organization_name,
    attribute2 as organization_description

from source
