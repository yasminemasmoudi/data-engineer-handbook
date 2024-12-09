-- Backfill query with simplified change tracking
WITH actors_source AS (
    SELECT DISTINCT ON (actorid, current_year)
        actor,
        actorid,
        current_year,
        quality_class,
        is_active
    FROM actors
    WHERE current_year IS NOT NULL
    ORDER BY actorid, current_year
),
change_markers AS (
    SELECT
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER w as prev_quality_class,
        LAG(is_active) OVER w as prev_is_active,
        CASE WHEN 
            LAG(quality_class) OVER w IS NULL 
            OR LAG(is_active) OVER w IS NULL 
            OR quality_class != LAG(quality_class) OVER w 
            OR is_active != LAG(is_active) OVER w
        THEN 1 ELSE 0 END as is_changed
    FROM actors_source
    WINDOW w AS (PARTITION BY actorid ORDER BY current_year)
),
change_detection AS (
    SELECT
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        is_changed,
        SUM(is_changed) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) as change_streak
    FROM change_markers
)
INSERT INTO actors_history_scd (
    actor,
    actorid,
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
    current_year as start_date,
    LEAD(current_year - 1) OVER (
        PARTITION BY actorid 
        ORDER BY current_year
    ) as end_date,
    CASE 
        WHEN LEAD(actorid) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) IS NULL THEN TRUE 
        ELSE FALSE 
    END as current_flag,
    change_streak
FROM change_detection
WHERE is_changed = 1  -- Only insert records where a change occurred
ORDER BY actorid, current_year;


SELECT * FROM actors_history_scd
ORDER BY actor;