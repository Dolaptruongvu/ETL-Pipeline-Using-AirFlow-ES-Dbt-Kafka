{{ config(materialized='incremental',
          unique_key='account_number'
) }}

with source_data as (
    select * from {{ source('public', 'accounts') }}
    {% if is_incremental() %}
       where account_number not in ( select account_number from {{this}})
    {% endif %}
)

select account_number
from source_data
where firstname is null
