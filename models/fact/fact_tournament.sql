{{config(schema='dwh')}}

{{
    config(        
        materialized='incremental',
        unique_key=['player_id','name','timestamp_utc'],        
        incremental_strategy = 'merge',
        merge_exclude_columns = ['created_at'],
        partition_by= { 'field': 'timestamp_utc',
                        'data_type': 'timestamp',
                        'granularity': 'day'}
    )
}}


with tournament_pop as (

SELECT *
FROM {{ref("stg_fact_tournament")}}
)

SELECT 
name,
player_id,
timestamp_utc,
tournament_score,
CURRENT_TIMESTAMP() as created_at,
CURRENT_TIMESTAMP() as updated_at
FROM tournament_pop