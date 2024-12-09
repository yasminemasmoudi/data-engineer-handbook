-- we want to create a table with 1 row per player and an array of seasons.
-- creting a STRUCT 
CREATE TYPE season_stats AS (
                        season Integer,
                        pts REAL,
                        ast REAL,
                        reb REAL,
                        weight INTEGER
                    );

CREATE TYPE scoring_class AS
    ENUM ('bad', 'average', 'good', 'star');

CREATE TABLE players (
    player_name                    TEXT,
    height                         TEXT,
    college                        TEXT,
    country                        TEXT,
    draft_year                     TEXT,
    draft_round                    TEXT,
    draft_number                   TEXT,
    seasons              season_stats[],
    scoring_class          scoring_class,
    years_since_last_season     INTEGER,
    current_season              INTEGER,
    is_active                   BOOLEAN,
    PRIMARY KEY (player_name, current_season)
);


WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 1995

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 1996
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number) as draft_number,

        COALESCE(ls.seasons, ARRAY[]::season_stats[]) || CASE WHEN ts.season IS NOT NULL 
                                                                THEN ARRAY[ROW( ts.season,
                                                                                ts.pts,
                                                                                ts.ast,
                                                                                ts.reb, ts.weight)::season_stats]
                                                                ELSE ARRAY[]::season_stats[]
                                                        END as seasons,

        CASE WHEN ts.season IS NOT NULL 
        THEN (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class

        ELSE ls.scoring_class
        END as scoring_class,

        CASE    WHEN ts.season IS NOT NULL THEN 0 
                ELSE ls.season.years_since_last_season + 1
        END AS years_since_last_season,

        COALESCE(ts.season , ls.current_season + 1) AS current_season

FROM last_season ls
FULL OUTER JOIN this_season ts
ON ls.player_name = ts.player_name

-- Unnest query 
SELECT player_name,
        UNNEST(seasons) -- CROSS JOIN UNNEST
        -- / LATERAL VIEW EXPLODE
FROM players
WHERE current_season = 1998
AND player_name = 'Michael Jordan';


--Another way to do it 
WITH unnested as (
    SELECT player_name, UNNEST(seasons)::season_stats AS seasons
    FROM players
    WHERE current_season = 1998
)
SELECT player_name, (seasons::season_stats).*
FROM unnested;

SELECT player_name,
    (seasons[cardinality(seasons)]::season_stats).pts/
        CASE WHEN (seasons[1]::season_stats).pts = 0 THEN 1
            ELSE  (seasons[1]::season_stats).pts END
        AS ratio_most_recent_to_first
FROM players
WHERE current_season = 1998;


-- insert players
INSERT INTO players (
    player_name,
    height,
    college,
    country,
    draft_year,
    draft_round,
    draft_number,
    seasons,
    scoring_class,
    years_since_last_season,
    is_active,
    current_season
)
WITH years AS (
    SELECT *
    FROM generate_series(1996, 2022) AS season
), 
p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), 

players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active,
    ((seasons[CARDINALITY(seasons)]::season_stats).season = w.season)::BOOLEAN AS is_active,
    w.season AS current_season
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;




