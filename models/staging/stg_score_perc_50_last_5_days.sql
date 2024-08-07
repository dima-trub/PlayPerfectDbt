WITH recent_scores AS (
    SELECT 
        player_id, 
        tournament_score,
        timestamp_utc
    FROM {{ref("fact_tournament")}} 
    WHERE name = 'srvTournamentFinished'
      AND DATE(timestamp_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
)
SELECT 
    player_id,
    CASE 
        WHEN COUNT(tournament_score) = 0 THEN NULL
        ELSE APPROX_QUANTILES(tournament_score, 2)[OFFSET(1)] 
    END AS score_perc_50_last_5_days
FROM 
    recent_scores
GROUP BY 
    player_id