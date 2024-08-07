
WITH deposits_pop AS (
    SELECT
      player_id,
      deposit_amount,
      LAST_VALUE(timestamp_utc)  OVER(PARTITION BY player_id ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_deposit_time,      
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY timestamp_utc DESC) AS rn
    FROM  {{ref("fact_player_transactions")}}
    WHERE deposit_amount IS NOT NULL
)   

SELECT
    player_id,
    AVG(deposit_amount) AS avg_price_10,
    max(date(last_deposit_time)) as last_purchase_date
FROM deposits_pop
WHERE rn <= 10
GROUP BY player_id