--2 cumulative table generation query

WITH years AS (
    SELECT * FROM GENERATE_SERIES(1970, 2021) AS year
), actors_first_year AS (
    SELECT 
        actor, 
        actorid, 
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor, actorid
), actors_and_years AS (
    SELECT 
        afy.actor, 
        afy.actorid,
        y.year AS current_year
    FROM actors_first_year afy
    JOIN years y
        ON afy.first_year <= y.year
), current_year_films AS (
    SELECT 
        ay.actor,
        ay.actorid,
        ay.current_year,
        ARRAY_AGG(
            ROW(
                af.film,
                af.year,
                af.votes,
                af.rating
            )::films 
        ORDER BY af.year) AS films,
        AVG(af.rating) AS avg_recent_rating 
    FROM actors_and_years ay
    LEFT JOIN actor_films af
        ON ay.actorid = af.actorid 
        AND ay.current_year = af.year
    GROUP BY ay.actor, ay.actorid, ay.current_year
)
INSERT INTO actors (actor, actorid, films, quality_class, is_active, avg_recent_rating, current_year)
SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actorid, cy.actorid) AS actorid,
    COALESCE(ly.films, ARRAY[]::films[]) || COALESCE(cy.films, ARRAY[]::films[]) AS films,
    CASE 
        WHEN COALESCE(cy.avg_recent_rating, ly.avg_recent_rating) > 8 THEN 'star'
        WHEN COALESCE(cy.avg_recent_rating, ly.avg_recent_rating) > 7 THEN 'good'
        WHEN COALESCE(cy.avg_recent_rating, ly.avg_recent_rating) > 6 THEN 'average'        
        ELSE 'bad'
    END::quality_class AS quality_class,
    cy.films IS NOT NULL AS is_active,
    COALESCE(cy.avg_recent_rating, ly.avg_recent_rating) AS avg_recent_rating, 
    cy.current_year AS current_year
FROM actors ly
FULL OUTER JOIN current_year_films cy
    ON ly.actorid = cy.actorid 
    AND ly.current_year = cy.current_year - 1;


SELECT 
    actor,
    actorid,
    current_year,
    ARRAY_LENGTH(films, 1) AS total_films, 
    quality_class,
    is_active,
    avg_recent_rating
FROM actors
ORDER BY current_year, actor;
