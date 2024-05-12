with

source as (

    select * from {{ source('rdp', 'ImpactStatement') }}

)

select
    impactstatementguid as impact_statement_id,
    impactstatement as impact_statement,
    impactstatementcode as impact_statement_code,
    impactstatementshortdescr as impact_statement_short_descr,
    impactstatementlongdescr as impact_statement_long_descr,
    impactareaguid as impact_area_id

from source

where isdeleted = 0
