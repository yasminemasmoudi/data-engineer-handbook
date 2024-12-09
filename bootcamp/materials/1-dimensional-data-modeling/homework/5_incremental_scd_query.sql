-- Incremental query for `actors_history_scd`
WITH latest_actors AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year AS start_date
    FROM actors
    WHERE current_year IS NOT NULL
),
current_records AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        current_flag,
        change_streak
    FROM actors_history_scd
    WHERE current_flag = TRUE
),
updated_records AS (
    SELECT
        cr.actorid,
        cr.actor,
        cr.start_date,
        cr.end_date,
        cr.current_flag,
        cr.change_streak,
        la.quality_class AS new_quality_class,
        la.is_active AS new_is_active,
        la.start_date AS new_start_date
    FROM current_records cr
    JOIN latest_actors la
        ON cr.actorid = la.actorid
    WHERE
        cr.quality_class != la.quality_class
        OR cr.is_active != la.is_active
),
close_previous_records AS (
    UPDATE actors_history_scd
    SET
        end_date = ur.new_start_date - 1,
        current_flag = FALSE
    FROM updated_records ur
    WHERE actors_history_scd.actorid = ur.actorid
    AND actors_history_scd.current_flag = TRUE
    RETURNING actors_history_scd.actorid
),
new_records AS (
    SELECT
        la.actorid,
        la.actor,
        la.quality_class,
        la.is_active,
        la.start_date,
        NULL::INTEGER AS end_date,
        TRUE AS current_flag,
        COALESCE(cr.change_streak, 0) + 1 AS change_streak
    FROM latest_actors la
    LEFT JOIN current_records cr
        ON la.actorid = cr.actorid
    WHERE NOT EXISTS (
        SELECT 1
        FROM actors_history_scd h
        WHERE h.actorid = la.actorid
        AND h.start_date = la.start_date
    )
)



INSERT INTO actors_history_scd (
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_flag,
    change_streak
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_flag,
    change_streak
FROM new_records
ON CONFLICT (actorid, start_date) DO NOTHING;

SELECT *
FROM actors_history_scd
ORDER BY actor, start_date;

