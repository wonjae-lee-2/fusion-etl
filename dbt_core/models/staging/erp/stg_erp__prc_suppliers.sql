with

source as (

    select
        timestamp_utc,
        vendor_id,
        segment1 as supplier_number,
        party_name as supplier_name,
        vendor_type_lookup_code as supplier_type,
        lookup_code as business_classification,
        attribute5 as employee_id

    from {{ source('erp', 'prc_suppliers') }}

)

select * from source
