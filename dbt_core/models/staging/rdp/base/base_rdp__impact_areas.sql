with

source as (

    select * from {{ source('rdp', 'ImpactArea') }}

)

select
    impactareaguid as impact_area_id,
    impactarea as impact_area,
    impactareashortdescr as impact_area_short_descr,
    impactarealongdescr as impact_area_long_descr

from source

where isdeleted = 0
