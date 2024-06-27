with

source as (

    select
        timestamp_utc,
        value as budget_situation_code,
        description as budget_situation,
        value + ' ' + description as budget_situation_tag

    from
        {{ source('erp', 'ppm_budget_situations') }}

)

select
    timestamp_utc,
    budget_situation_code,
    budget_situation,
    budget_situation_tag

from source
