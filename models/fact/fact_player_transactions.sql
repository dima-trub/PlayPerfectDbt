{{config(schema='dwh')}}


{{
    config(        
        materialized='incremental',
        unique_key=['player_id','timestamp_utc'],        
        incremental_strategy = 'merge',
        merge_exclude_columns = ['created_at'],
        partition_by= { 'field': 'timestamp_utc',
                        'data_type': 'timestamp',
                        'granularity': 'day'}
    )
}}


with player_transactions_pop as (

SELECT *
FROM {{ref("stg_fact_player_transactions")}}
)

SELECT 
player_id,
timestamp_utc,
deposit_amount,
CURRENT_TIMESTAMP() as created_at,
CURRENT_TIMESTAMP() as updated_at
FROM player_transactions_pop