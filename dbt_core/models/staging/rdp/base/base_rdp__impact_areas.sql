with

source as (

    select
        impactareaguid as impact_area_id,
        impactarea as impact_area_code,
        impactareashortdescr as impact_area,
        impactarealongdescr as impact_area_description

    from {{ source('rdp', 'ImpactArea') }}

    where isdeleted = 0

)

select * from source
