from psycopg2 import sql

dim_teams_extract = sql.SQL(
    """
SELECT "uuid", id_team, name_team, created_at, updated_at, team_logo
FROM soccer.dim_teams;
"""
)

update_dim_teams_query = sql.SQL(
    """
    UPDATE soccer.dim_teams AS f
    SET name_team = t.name_team,
    team_logo = t.team_logo::bytea,
    updated_at = t.updated_at::timestamp
    FROM cte AS t
    WHERE f.uuid::text = t.uuid::text;
    """
)

dim_league_extract = sql.SQL(
    """
SELECT "uuid", id_league, country, name_league, league_logo, country_flag, created_at, updated_at
FROM soccer.dim_league;
"""
)

update_dim_league_query = sql.SQL(
    """
    UPDATE soccer.dim_league AS f
    SET name_league = t.name_league,
    country = t.country,
    league_logo = t.league_logo::bytea,
    country_flag = t.country_flag::bytea,
    updated_at = t.updated_at::timestamp
    FROM cte AS t
    WHERE f.uuid::text = t.uuid::text;
    """
)

fact_matches_extract = sql.SQL(
    """
SELECT id_match
FROM soccer.fact_matches;
"""
)

fact_matches_statistics_extract = sql.SQL(
    """
SELECT id_match,date_match,fm."uuid"
FROM soccer.fact_matches fm 
LEFT JOIN soccer.fact_matches_statistics fms ON fm."uuid" = fms.uuid_match 
WHERE fms."uuid" IS NULL
ORDER BY 2 DESC
LIMIT 30
"""
)
