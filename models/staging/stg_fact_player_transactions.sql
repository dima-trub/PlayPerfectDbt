WITH increment AS (

  {{ get_increment_column('dbt_dimatrub', 'fact_player_transactions','timestamp_utc') }}

),
deposit_amount_pop as (
SELECT  
    player_id,
    timestamp_utc,
    deposit_amount

FROM {{ source('raw_data', 'raw_data') }}
WHERE name IN ('srvTransactionCompleted')
AND timestamp_utc BETWEEN (SELECT increment_date FROM increment) AND (SELECT increment_date FROM increment) + INTERVAL 60 DAY
qualify ROW_NUMBER() OVER (PARTITION BY player_id, timestamp_utc ORDER BY timestamp_utc DESC)=1
)

SELECT *
FROM deposit_amount_pop