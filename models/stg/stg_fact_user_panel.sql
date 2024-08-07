with raw_data as 
(
  SELECT *
  FROM {{ref("stg_user_panel_raw_data")}}

),
avg_price_10_pop as 
(

SELECT *
FROM {{ref("stg_avg_price_10")}}

),

last_weighted_daily_matches_count_10_played_days as 
(

SELECT *
FROM {{ref("stg_last_weighted_daily_matches_count_10_played_days")}}

),

score_perc_50_last_5_days as 
(
    
SELECT *
FROM {{ref("stg_score_perc_50_last_5_days")}}  

)

SELECT 
rd.player_id,
rd.country as country,
avg_pr.avg_price_10,
lwdm.last_weighted_daily_matches_count_10_played_days,
CASE
  WHEN avg_pr.last_purchase_date = CURRENT_DATE() THEN 0
  ELSE DATE_DIFF( avg_pr.last_purchase_date,date(rd.player_last_seen_time), DAY) 
END AS active_days_since_last_purchase,
scrp50.score_perc_50_last_5_days,       
rd.player_last_seen_time
FROM raw_data as rd
LEFT JOIN avg_price_10_pop as avg_pr
ON rd.player_id = avg_pr.player_id
LEFT JOIN last_weighted_daily_matches_count_10_played_days as lwdm
ON rd.player_id = lwdm.player_id
LEFT JOIN score_perc_50_last_5_days as scrp50
ON rd.player_id = scrp50.player_id 




