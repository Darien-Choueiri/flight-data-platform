-- Display results in terminal not in scrollable viewer
\pset pager off

-- Measure query time 
\timing on

\echo Ten busiest origin airports
SELECT airport_code, flight_count
FROM batch_airport_counts
WHERE airport_role = 'origin'
ORDER BY flight_count DESC, airport_code
LIMIT 10;

\echo Ten busiest destination airports
SELECT airport_code, flight_count
FROM batch_airport_counts
WHERE airport_role = 'destination'
ORDER BY flight_count DESC, airport_code
LIMIT 10;

\echo Total flights in summer for each year
SELECT flight_year, flight_count
FROM batch_year_counts
ORDER BY flight_year;

\echo Top five airport-hours in UTC
SELECT airport_code, hour_start AS hour_start_utc, flight_count
FROM stream_airport_hour_counts
ORDER BY flight_count DESC, airport_code, hour_start
LIMIT 5;