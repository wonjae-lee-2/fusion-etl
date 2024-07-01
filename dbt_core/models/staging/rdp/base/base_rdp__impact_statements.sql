with

source as (

    select
        impactstatementguid as impact_statement_id,
        impactstatement as impact_statement_short_code,
        impactstatementcode as impact_statement_long_code,
        impactstatementshortdescr as impact_statement,
        impactstatementlongdescr as impact_statement_description,
        impactareaguid as impact_area_id

    from {{ source('rdp', 'ImpactStatement') }}

    where isdeleted = 0

)

select * from source
