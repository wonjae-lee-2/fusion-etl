with

source as (

    select * from {{ source('erp', 'FND_VS_VALUES_B') }}

)

select
    job_utc_timestamp,
    attribute_category,
    value,
    description

from source
