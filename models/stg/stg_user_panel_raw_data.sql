
WITH increment AS (
  {{ get_increment_column('dbt_dimatrub', 'fact_user_panel','player_last_seen_time') }}
),
filtered_data AS (
  SELECT 
    *
  FROM 
    {{ source('raw_data', 'raw_data') }}  -- Adjust this source if needed
  WHERE 
    timestamp_utc BETWEEN (SELECT increment_date FROM increment) 
                       AND (SELECT increment_date FROM increment) + INTERVAL 60 DAY
)

SELECT distinct
player_id,
LAST_VALUE(country)  OVER(PARTITION BY player_id ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS country,
LAST_VALUE(timestamp_utc)  OVER(PARTITION BY player_id ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS player_last_seen_time
 FROM filtered_data
