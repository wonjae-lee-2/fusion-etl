with

budget_situations as (

    select
        job_utc_timestamp,
        value,
        description

    from {{ ref('base_erp__fnd_vs_values_b') }}

    where attribute_category = 'HCR_Budget_Situation'

)

select
    job_utc_timestamp,
    value as budget_situation_code,
    description as budget_situation,
    value + ' ' + description as budget_situation_tag

from budget_situations
