with

outcome_areas as (

    select
        outcome_area_id,
        outcome_area_code,
        outcome_area,
        outcome_area_description

    from {{ ref('base_rdp__outcome_areas') }}

)

select * from outcome_areas
