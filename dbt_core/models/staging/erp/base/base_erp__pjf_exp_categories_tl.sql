with

source as (

    select * from {{ source('erp', 'PJF_EXP_CATEGORIES_TL') }}
)

select
    job_utc_timestamp,
    expenditure_category_id,
    expenditure_category_name as expenditure_category,
    case left(expenditure_category_name, 1)
        when 'B' then left(expenditure_category_name, 4)
    end as expenditure_category_code

from source
