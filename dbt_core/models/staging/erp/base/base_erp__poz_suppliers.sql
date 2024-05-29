with

source as (

    select * from {{ source('erp', 'POZ_SUPPLIERS') }}
)

select
    job_utc_timestamp,
    vendor_id,
    segment1 as supplier_number,
    party_name as supplier_name,
    vendor_type_lookup_code as supplier_type,
    lookup_code as supplier_business_classification

from source
