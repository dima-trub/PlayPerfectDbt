
WITH increment AS (

  {{ get_increment_column('dbt_dimatrub', 'fact_tournament','timestamp_utc') }}

),
tournament_pop as (

SELECT  
   name,
   player_id,
   timestamp_utc,
   tournament_score

FROM {{ source('raw_data', 'raw_data') }}
WHERE name IN ('srvTournamentJoined','srvTournamentFinished')
AND timestamp_utc BETWEEN (SELECT increment_date FROM increment) AND (SELECT increment_date FROM increment) + INTERVAL 60 DAY
qualify ROW_NUMBER() OVER (PARTITION BY player_id,name, timestamp_utc ORDER BY timestamp_utc DESC)=1
)

SELECT *
FROM tournament_pop

