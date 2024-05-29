with

source as (

    select * from {{ source('erp', 'PJF_TXN_SOURCES_B' ) }}

)

select
    job_utc_timestamp,
    transaction_source_id as source_id,
    transaction_source as source_code,
    user_transaction_source as source_name,
    description as source_description

from source

-- exclude Oracle Fusion Time and Labor
-- to make document_code in pjf_txn_sources_tl unique
where transaction_source_id <> '300000000442205'
