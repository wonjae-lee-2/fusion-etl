with

source as (

    select
        timestamp_utc,
        tree_version_name,
        distance,
        dep24_pk1_value as expenditure_organization_id,
        dep30_pk1_name as field_hq,
        dep29_pk1_name as region,
        dep28_pk1_name as subregion,
        dep27_pk1_name as operation,
        dep26_pk1_name as business_unit,
        dep25_pk1_name as suboffice,
        dep24_pk1_name as expenditure_organization,
        dep24_pk1_attribute2 as expenditure_organization_description

    from {{ source('erp', 'ppm_organizations') }}
)

select * from source
