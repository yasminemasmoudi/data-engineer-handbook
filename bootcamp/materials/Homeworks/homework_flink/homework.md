# Homework

- Create a Flink job that sessionizes the input data by IP address and host
- Use a 5 minute gap
- Answer these questions
  - What is the average number of web events of a session from a user on Tech Creator?
  - 
  

# Solution

## The job:
* Reads web traffic events from Kafka
* Groups events into sessions using a 5-minute inactivity gap
* Aggregates session statistics by host and IP
* Stores results in PostgreSQL
* Analyzes average web events per session, particularly for Tech Creator domains

### Job Details
* Sessions are grouped by IP address and host
* A new session starts after 5 minutes of inactivity
* Each session tracks: the Start timestamp, Host, IP address and Number of hits

## Environment variables
### Required Kafka Configuration
KAFKA_GROUP=<consumer-group-id>
KAFKA_TOPIC=<topic-name>
KAFKA_URL=<broker-url>
KAFKA_WEB_TRAFFIC_KEY=<kafka-key>
KAFKA_WEB_TRAFFIC_SECRET=<kafka-secret>

### Required PostgreSQL Configuration
POSTGRES_URL=<jdbc-url>
POSTGRES_USER=<username>
POSTGRES_PASSWORD=<password>

## Setup
 1. Create flink-env.env file with required environment variables
 2. Start the Flink cluster: `docker compose --env-file flink-env.env up --build --remove-orphans  -d`
 3. Execute the Flink job using: `docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregated_ip_job.py -d`


## Results Summary

### Active Domains
1. bootcamp.techcreator.io
   - Average hits: 2.1 per session
   - Peak activity: 6 hits per session
   
2. www.dataexpert.io
   - Average hits: 3.4 per session
   - Peak activity: 9 hits per session

### Monitored Domains (No Activity)
- zachwilson.techcreator.io
- lulu.techcreator.io
- zachwilson.tech

Note: Consider investigating data ingestion for monitored domains.


3. bootcamp.techcreator.io:

-- Average hits per session varies from 1 to 6
-- Notable sessions: IP 37.63.23.137 with 6 hits, IP 66.96.225.158 with 5 hits


4. www.dataexpert.io:

-- More active sessions overall
-- Higher hit counts: up to 9 hits (IP 158.222.144.26)
-- More consistent activity pattern
