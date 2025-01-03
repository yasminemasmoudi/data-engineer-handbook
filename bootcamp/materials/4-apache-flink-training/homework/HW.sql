CREATE TABLE processed_events_ip_aggregated_source(
	event_window_timestamp TIMESTAMP(3),
    host VARCHAR,
    ip VARCHAR,
    num_hits BIGINT
)


-- SELECT * FROM processed_events_ip_aggregated_source

--1)What is the average number of web events of a session from a user on Tech Creator?
--==>the average number of hits is 1 but there are lots that do have multiple hits.
--get average number of hits per session
SELECT 
		host,
		CAST (AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated_source
WHERE host LIKE '%techcreator.io'
GROUP BY host
-- example of result 
-- "www.techcreator.io"	1
-- "bootcamp.techcreator.io"	3


SELECT 
		host,
		CAST (AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated_source
GROUP BY host
-- example of result 
-- "www.dataexpert.io"	3
-- "www.techcreator.io"	1
-- "bootcamp.techcreator.io"	3
-- "www.linkedinexpert.io"	2

--2)Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
--get average number of hits per session per site

SELECT 
    ip,
    CASE host
        WHEN 'zachwilson.techcreator.io' THEN 'Zach TC'
        WHEN 'lulu.techcreator.io' THEN 'Lulu TC'
        WHEN 'zachwilson.tech' THEN 'Zach Tech'
    END as site,
    event_window_timestamp,
    CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated_source
WHERE host IN ('zachwilson.techcreator.io', 'lulu.techcreator.io', 'zachwilson.tech')
GROUP BY 
    ip,
    host,
    event_window_timestamp
ORDER BY 
    event_window_timestamp DESC;

-- No activity for originally requested domains:
-- zachwilson.techcreator.io
-- lulu.techcreator.io
-- zachwilson.tech

-- However, we do have good data for:

SELECT ip,
		host,
		event_window_timestamp,
		CAST (AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated_source
WHERE host = 'bootcamp.techcreator.io'
GROUP BY ip, host,event_window_timestamp
-- some results of the query:
-- "71.150.218.94"	"bootcamp.techcreator.io"	"2025-01-03 21:37:47.125"	2
-- "66.96.225.158"	"bootcamp.techcreator.io"	"2025-01-03 21:50:00"	5
-- "86.163.247.28"	"bootcamp.techcreator.io"	"2025-01-03 21:45:00"	1
-- "117.213.200.2"	"bootcamp.techcreator.io"	"2025-01-03 21:35:00"	1
-- "107.192.240.179"	"bootcamp.techcreator.io"	"2025-01-03 21:45:38.553"	4
-- "181.223.200.114"	"bootcamp.techcreator.io"	"2025-01-03 21:50:00"	1
-- "178.27.168.218"	"bootcamp.techcreator.io"	"2025-01-03 21:40:00"	1
-- "68.39.148.143"	"bootcamp.techcreator.io"	"2025-01-03 21:35:00"	1
-- "31.151.164.50"	"bootcamp.techcreator.io"	"2025-01-03 21:35:00"	1
-- "37.63.23.137"	"bootcamp.techcreator.io"	"2025-01-03 21:45:00"	6
-- "205.254.175.60"	"bootcamp.techcreator.io"	"2025-01-03 21:40:00"	1
-- "178.27.168.218"	"bootcamp.techcreator.io"	"2025-01-03 21:40:29.531"	1
-- "99.230.72.186"	"bootcamp.techcreator.io"	"2025-01-03 21:38:14.33"	4
-- "206.108.31.38"	"bootcamp.techcreator.io"	"2025-01-03 21:30:00"	2
-- "46.40.231.164"	"bootcamp.techcreator.io"	"2025-01-03 21:47:53.982"	2
-- "205.254.175.60"	"bootcamp.techcreator.io"	"2025-01-03 21:41:11.891"	1
-- "206.108.31.38"	"bootcamp.techcreator.io"	"2025-01-03 21:40:00"	1
-- "73.130.200.28"	"bootcamp.techcreator.io"	"2025-01-03 21:35:00"	4


SELECT ip,
		host,
		event_window_timestamp,
		CAST (AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated_source
WHERE host = 'www.dataexpert.io'
GROUP BY ip, host,event_window_timestamp

-- some results of the query:
-- "68.150.193.112"	"www.dataexpert.io"	"2025-01-03 21:48:33.599"	3
-- "191.96.103.75"	"www.dataexpert.io"	"2025-01-03 21:41:02.094"	1
-- "179.14.168.25"	"www.dataexpert.io"	"2025-01-03 21:50:00"	8
-- "70.111.84.80"	"www.dataexpert.io"	"2025-01-03 21:47:48.659"	5
-- "158.222.144.26"	"www.dataexpert.io"	"2025-01-03 21:45:46.294"	9
-- "204.236.164.172"	"www.dataexpert.io"	"2025-01-03 21:43:46.38"	2
-- "84.253.239.174"	"www.dataexpert.io"	"2025-01-03 21:35:00"	4
-- "42.108.75.212"	"www.dataexpert.io"	"2025-01-03 21:35:00"	6
-- "69.214.91.188"	"www.dataexpert.io"	"2025-01-03 21:45:00"	2
-- "204.63.43.37"	"www.dataexpert.io"	"2025-01-03 21:50:00"	1
-- "104.51.50.226"	"www.dataexpert.io"	"2025-01-03 21:40:00"	2
-- "37.201.144.245"	"www.dataexpert.io"	"2025-01-03 21:40:00"	3
-- "152.117.115.147"	"www.dataexpert.io"	"2025-01-03 21:35:00"	7
-- "73.97.192.242"	"www.dataexpert.io"	"2025-01-03 21:30:00"	1
-- "191.96.103.75"	"www.dataexpert.io"	"2025-01-03 21:40:00"	1
-- "73.71.254.55"	"www.dataexpert.io"	"2025-01-03 21:50:46.405"	1
-- "96.8.253.110"	"www.dataexpert.io"	"2025-01-03 21:45:00"	2
-- "152.117.115.147"	"www.dataexpert.io"	"2025-01-03 21:36:44.311"	7
-- "83.87.96.188"	"www.dataexpert.io"	"2025-01-03 21:48:21.556"	2

--==>www.dataexpert.io, bootcamp.techcreator.io both have the most hits for their site

-- bootcamp.techcreator.io:

-- Average hits per session varies from 1 to 6
-- Notable sessions: IP 37.63.23.137 with 6 hits, IP 66.96.225.158 with 5 hits


-- www.dataexpert.io:

-- More active sessions overall
-- Higher hit counts: up to 9 hits (IP 158.222.144.26)
-- More consistent activity pattern



