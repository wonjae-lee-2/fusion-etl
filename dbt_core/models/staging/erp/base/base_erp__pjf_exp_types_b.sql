with

source as (

    select * from {{ source('erp', 'PJF_EXP_TYPES_B') }}
)

select
    job_utc_timestamp,
    expenditure_type_id,
    expenditure_category_id,
    expenditure_type_name as expenditure_type,
    case isnumeric(left(expenditure_type_name, 6))
        when 1 then left(expenditure_type_name, 6)
    end as expenditure_type_code

from source
