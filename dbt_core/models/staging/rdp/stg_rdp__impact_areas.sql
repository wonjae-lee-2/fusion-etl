with

impact_areas as (

    select
        impact_area_id,
        impact_area_code,
        impact_area,
        impact_area_description

    from {{ ref('base_rdp__impact_areas') }}

)

select * from impact_areas
