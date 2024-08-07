{{config(schema='stg')}}

WITH daily_matches AS (
    SELECT
        player_id,
        DATE(timestamp_utc) AS date,
        COUNT(*) AS matches
    FROM {{ref("fact_tournament")}}   
    WHERE name = 'srvTournamentJoined'
    GROUP BY player_id, date
),

weighted_matches AS (
    SELECT
        player_id,
        SUM(matches * rn) / SUM(rn) AS last_weighted_daily_matches_count_10_played_days
    FROM (
        SELECT
            player_id,
            matches,
            ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY date DESC) AS rn
        FROM daily_matches
    ) 
    WHERE rn <= 10
    GROUP BY player_id
)

SELECT 
player_id,
last_weighted_daily_matches_count_10_played_days
FROM weighted_matches