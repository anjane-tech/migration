{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy = 'merge')
}}

select *, current_timestamp() as _update_dt
from {{ ref('first_dbt_model') }}
