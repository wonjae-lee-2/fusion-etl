with

source as (

    select * from {{ source('erp', 'FND_LOOKUP_VALUES_TL') }}

)

select
    job_utc_timestamp,
    lookup_type,
    lookup_code,
    meaning,
    description

from source

where
    lookup_type in (
        'XCC_BALANCE_ACTIVITY_TYPES',
        'XCC_FUNDS_AVAIL_BAL_BUCKETS',
        'XCC_RESERV_CATEGORY_BUCKETS',
        'XCC_TRANSACTION_TYPES',
        'XCC_TRANSACTION_SUBTYPES',
        'XCC_TRANSACTION_TYPE_ACTION',
        'XCC_RECVING_DEST_TYPE'
    )
