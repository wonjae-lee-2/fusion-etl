with

source as (

    select
        timestamp_utc,
        doc_entry_id,
        user_transaction_source as transaction_source,
        document_name as document,
        doc_entry_name as document_entry

    from {{ source('erp', 'ppm_sources') }}

)

select * from source
