with

source as (

    select * from {{ source('rdp', 'OutcomeArea') }}

)

select
    outcomeareaguid as outcome_area_id,
    outcomearea as outcome_area,
    outcomeareashortdescr as outcome_area_short_descr,
    outcomearealongdescr as outcome_area_long_descr

from source

where isdeleted = 0
