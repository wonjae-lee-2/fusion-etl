with

source as (

    select
        timestamp_utc,
        expenditure_type_id,
        expenditure_category_name as expenditure_category,
        expenditure_type_name as expenditure_type

    from {{ source('erp', 'ppm_expenditure_categories') }}

)

select * from source
