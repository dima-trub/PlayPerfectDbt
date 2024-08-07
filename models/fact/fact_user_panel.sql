{{config(schema='dwh')}}

{{
    config(        
        materialized='incremental',
        unique_key=['player_id'],        
        incremental_strategy = 'merge',
        merge_exclude_columns = ['created_at'],
        partition_by= { 'field': 'player_last_seen_time',
                        'data_type': 'timestamp',
                        'granularity': 'day'}
    )
}}


with user_panel_pop as (

SELECT *
FROM {{ref("stg_fact_user_panel")}}
)

SELECT 
player_id,
country,
avg_price_10,
last_weighted_daily_matches_count_10_played_days,
active_days_since_last_purchase,
score_perc_50_last_5_days,       
player_last_seen_time,
CURRENT_TIMESTAMP() as created_at,
CURRENT_TIMESTAMP() as updated_at
FROM user_panel_pop