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

),

exceptions (
    timestamp_utc,
    vendor_id,
    supplier_number,
    supplier_name,
    supplier_type,
    business_classification,
    employee_id
) as (

    select
        cast('2023-01-01 00:00:00' as datetime2(0)),
        '8999999',
        '8999999',
        'Spending Authority Adjustment',
        'Partners & DI',
        null,
        null

    union all

    select
        cast('2023-01-01 00:00:00' as datetime2(0)),
        '0000001',
        '0000001',
        'All Implementing Partners',
        'Unallocated',
        null,
        null

    union all

    select
        cast('2023-01-01 00:00:00' as datetime2(0)),
        '0000002',
        '0000002',
        'Direct Implementation',
        'Unallocated',
        null,
        null

)

select * from source

union all

select * from exceptions
