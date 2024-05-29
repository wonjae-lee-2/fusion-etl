with

source as (

    select * from {{ source('erp', 'PJF_TXN_DOCUMENT_B' ) }}

)

select
    job_utc_timestamp,
    document_id,
    transaction_source_id as source_id,
    document_code,
    document_name,
    description as document_description

from source

-- exclude Oracle Fusion Time and Labor
-- to make document_code in pjf_txn_sources_tl unique
where transaction_source_id <> '300000000442205'
