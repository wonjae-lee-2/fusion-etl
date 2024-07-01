with

source as (

    select
        outcomeareaguid as outcome_area_id,
        outcomearea as outcome_area_code,
        outcomeareashortdescr as outcome_area,
        outcomearealongdescr as outcome_area_description

    from {{ source('rdp', 'OutcomeArea') }}

    where isdeleted = 0

)

select * from source
