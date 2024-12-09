--1) DDL for actors table


CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
	current_year INTEGER,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
	avg_recent_rating REAL
);