create table players_scd
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season)
)

WITH streak_started AS (
SELECT player_name,
        current_season,
        scoring_class,
        LAG(scoring_class, 1) OVER
            (PARTITION BY player_name ORDER BY current_season) <> scoring_class
            OR LAG(scoring_class, 1) OVER
            (PARTITION BY player_name ORDER BY current_season) IS NULL
            AS did_change
FROM players
),
    streak_identified AS (
        SELECT
        player_name,
            scoring_class,
            current_season,
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
        FROM streak_started
    ),
    aggregated AS (
        SELECT
        player_name,
        scoring_class,
        streak_identifier,
        MIN(current_season) AS start_date,
        MAX(current_season) AS end_date
        FROM streak_identified
        GROUP BY 1,2,3
    )

SELECT player_name, scoring_class, start_date, end_date
FROM aggregated


CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        )


WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
historical_scd AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2020
    AND end_season < 2020
),
this_season_data AS (
    SELECT * FROM players
    WHERE current_season = 2021
),
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season as end_season 
    FROM this_season_data ts
    JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
    AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) as records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class <> ls.scoring_class
    OR ts.is_active <> ls.is_active
),
unnested_changed_records AS (
    SELECT
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season AS end_season 
    FROM changed_records
),
new_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT *
FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) a;




























WITH all_years AS (
    SELECT generate_series(1970, 2021) AS season
),
player_years AS (
    SELECT DISTINCT
        player_name,
        y.season
    FROM all_years y
    CROSS JOIN (SELECT DISTINCT player_name FROM players) p
),
historical_scd AS (
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE end_season < 2021
),
this_season_data AS (
    SELECT *
    FROM players
    WHERE current_season = 2021
),
last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
all_player_seasons AS (
    SELECT
        py.player_name,
        py.season,
        COALESCE(p.scoring_class, 'bad') AS scoring_class,
        COALESCE(p.is_active, FALSE) AS is_active
    FROM player_years py
    LEFT JOIN players p
    ON py.player_name = p.player_name
    AND py.season = p.current_season
),
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
    AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) AS records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class <> ls.scoring_class
    OR ts.is_active <> ls.is_active
),
unnested_changed_records AS (
    SELECT
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    FROM changed_records
),
new_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT *
FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
    UNION ALL
    SELECT player_name, scoring_class, is_active, season AS start_season, season AS end_season
    FROM all_player_seasons
) a
ORDER BY player_name, start_season;
