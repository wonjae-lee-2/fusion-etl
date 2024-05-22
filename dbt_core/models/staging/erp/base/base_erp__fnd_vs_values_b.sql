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

where
    attribute_category in (
        'HCR_COA_ACCOUNT',
        'HCR_COA_FUND',
        'HCR_COA_BUDGETCATEGORY',
        'HCR_COA_COSTCENTER',
        'HCR_Budget_Situation'
    )
