-- 1) A query to deduplicate `game_details` from Day 1 so there's no duplicates

SELECT game_id, team_id, player_id
FROM (
    SELECT 
        game_id, 
        team_id, 
        player_id, 
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
    FROM game_details
) ranked_game_details
WHERE row_num = 1;


-- 2) A DDL for an `user_devices_cumulated` table that has:
-- a `device_activity_datelist` which tracks a users active days by `browser_type`
-- data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
-- or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

-- SELECT * FROM events;
-- SELECT * FROM devices;

-- SELECT MAX(event_time), MIN(event_time)
-- FROM events; -- MAX 2023-01-31    --MIN 2023-01-01

CREATE TABLE user_devices_cumulated(
	user_id NUMERIC,
	device_activity_datelist JSONB, --the list of dates in the past 
	browser_type TEXT,
	date DATE, --the current date
	PRIMARY KEY (user_id, browser_type, date)
);

SELECT * FROM user_devices_cumulated;

-- 3) A cumulative query to generate `device_activity_datelist` from `events`

INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated WHERE date = current_date
), today AS (
    SELECT 
        e.user_id,
        ARRAY[DATE(e.event_time::TIMESTAMP)] AS today_dates,
        d.browser_type,
        DATE(e.event_time::TIMESTAMP) AS date
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE(e.event_time::TIMESTAMP) = current_date + INTERVAL '1 day'
    GROUP BY e.user_id, d.browser_type, DATE(e.event_time::TIMESTAMP)
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE 
        WHEN y.device_activity_datelist IS NULL THEN t.today_dates
        ELSE y.device_activity_datelist || t.today_dates  -- Cumulate dates
    END AS device_activity_datelist,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type;

SELECT * FROM user_devices_cumulated;


-- 4) A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

WITH series AS (
    SELECT generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')::DATE AS series_date
), place_holder_ints AS (
    SELECT 
        u.user_id,
        u.browser_type,
        u.date,
        CASE 
            WHEN series_date = ANY(u.device_activity_datelist) THEN 
                CAST(POW(2, 31 - (u.date - series_date)) AS BIGINT) 
            ELSE 0
        END AS placeholder_int_value
    FROM user_devices_cumulated u
    CROSS JOIN series
)
SELECT 
    user_id, 
    browser_type, 
    date, 
    SUM(placeholder_int_value) AS datelist_int,
	CAST(CAST (SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int_binary
FROM place_holder_ints
GROUP BY user_id, browser_type, date;

-- 5) A DDL for `hosts_cumulated` table 
-- a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
PRIMARY KEY (host, date)
);


-- 6) The incremental query to generate `host_activity_datelist`

DO $$ 
DECLARE
    start_date DATE := '2022-12-31';  -- Start date for the activity logging
    end_date DATE := '2023-01-31';    -- End date for the activity logging
    current_date DATE := start_date;  -- Variable to keep track of the current date being processed
BEGIN
    WHILE current_date <= end_date LOOP  -- Loop through each day in the date range
        INSERT INTO hosts_cumulated
        WITH yesterday AS (
            SELECT *  -- Select previous day's data for the given host
            FROM hosts_cumulated
            WHERE date = current_date
        ), today AS (
            SELECT 
                e.host,  -- Host where the activity took place
                ARRAY[DATE(e.event_time::TIMESTAMP)] AS today_dates,  -- Activity dates for the current day
                DATE(e.event_time::TIMESTAMP) AS date  -- Current date being processed
            FROM events e  -- Assume event logs are stored in the `events` table
            WHERE DATE(e.event_time::TIMESTAMP) = current_date + INTERVAL '1 day'  -- Filter by current date
            GROUP BY e.host, DATE(e.event_time::TIMESTAMP)  -- Group by host and event date
        )
        -- Merge today's data with yesterday's data
        SELECT 
            COALESCE(t.host, y.host) AS host,  -- If there was no activity yesterday, take today's host value
            CASE 
                WHEN y.host_activity_datelist IS NULL THEN t.today_dates  -- If no previous dates, start fresh
                ELSE y.host_activity_datelist || t.today_dates  -- Cumulate previous dates with today's dates
            END AS host_activity_datelist,
            COALESCE(t.date, y.date + INTERVAL '1 day') AS date  -- Ensure that the date is correct (yesterday or today)
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.host = y.host;  -- Join by host to ensure we're merging the same host records

        -- Increment the date by one day for the next iteration
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END $$;

SELECT * FROM hosts_cumulated;



-- 7) A monthly, reduced fact table DDL `host_activity_reduced`
-- month
-- host
-- hit_array - think COUNT(1)
-- unique_visitors array -  think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],      -- think COUNT(1)  
    unique_visitors_array INTEGER[], --  think COUNT(DISTINCT user_id)
    PRIMARY KEY (month, host)
);



-- 8) An incremental query that loads `host_activity_reduced`
-- day-by-day
DO $$ 
DECLARE
    start_date DATE := '2023-01-01';  -- Start date for daily processing
    end_date DATE := '2023-01-31';    -- End date for the range of daily updates
    current_date DATE := start_date;  -- Variable to keep track of the current date
BEGIN
    WHILE current_date <= end_date LOOP  -- Loop through each day in the range
        INSERT INTO host_activity_reduced
        WITH daily_aggregates AS (
            SELECT 
                e.host,  -- Host being analyzed
                DATE_TRUNC('month', DATE(e.event_time::TIMESTAMP)) AS month,  -- Truncate to the start of the month
                DATE(e.event_time::TIMESTAMP) AS day,  -- Specific day of activity
                COUNT(*) AS daily_hits,  -- Count of hits (total events for the day)
                COUNT(DISTINCT e.user_id) AS daily_unique_visitors  -- Count of unique users for the day
            FROM events e  -- Assume the event table stores activity data
            WHERE DATE(e.event_time::TIMESTAMP) = current_date  -- Only consider today's date
            GROUP BY e.host, DATE_TRUNC('month', DATE(e.event_time::TIMESTAMP)), DATE(e.event_time::TIMESTAMP)  -- Group by host and month
        ), 
        existing_monthly AS (
            SELECT *  -- Select the existing records for the current month and host from the `host_activity_reduced` table
            FROM host_activity_reduced
            WHERE month = DATE_TRUNC('month', current_date)  -- Filter by the current month
        )
        -- Merge the new daily aggregates with the existing monthly data
        SELECT 
            COALESCE(da.month, em.month) AS month,  -- Ensure month consistency
            COALESCE(da.host, em.host) AS host,    -- Ensure host consistency
            CASE 
                WHEN em.hit_array IS NOT NULL THEN em.hit_array || ARRAY[COALESCE(da.daily_hits, 0)]  -- Append today's hit count to existing array
                ELSE ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.day)::INTEGER - 1]) || ARRAY[COALESCE(da.daily_hits, 0)]  -- If no prior data, create a new array
            END AS hit_array,  -- Accumulate daily hits in an array
            CASE 
                WHEN em.unique_visitors_array IS NOT NULL THEN em.unique_visitors_array || ARRAY[COALESCE(da.daily_unique_visitors, 0)]  -- Append today's unique visitors
                ELSE ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.day)::INTEGER - 1]) || ARRAY[COALESCE(da.daily_unique_visitors, 0)]  -- If no prior data, create a new array
            END AS unique_visitors_array  -- Accumulate unique visitors in an array
        FROM daily_aggregates da
        FULL OUTER JOIN existing_monthly em
        ON da.host = em.host AND da.month = em.month  -- Ensure matching host and month for merging
        ON CONFLICT (month, host)  -- Handle conflicts when the same host and month already exist
        DO UPDATE SET 
            hit_array = 
                CASE 
                    WHEN EXCLUDED.hit_array IS NOT NULL THEN EXCLUDED.hit_array  -- If the new data exists, use it
                    ELSE host_activity_reduced.hit_array  -- Otherwise, retain existing data
                END,
            unique_visitors_array = 
                CASE 
                    WHEN EXCLUDED.unique_visitors_array IS NOT NULL THEN EXCLUDED.unique_visitors_array  -- Same for unique visitors
                    ELSE host_activity_reduced.unique_visitors_array
                END;

        -- Increment the date by one day for the next iteration
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END $$;
