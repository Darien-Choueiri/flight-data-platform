BEGIN;

-- Delete the tables in the case they might be populated
TRUNCATE TABLE batch_airport_counts, batch_year_counts, stream_airport_hour_counts;

\copy batch_airport_counts (airport_role, airport_code, flight_count) FROM '/app/batch_exports/batch_airport_counts.csv' WITH (FORMAT csv, HEADER true)
\copy batch_year_counts (flight_year, flight_count) FROM '/app/batch_exports/batch_year_counts.csv' WITH (FORMAT csv, HEADER true)
\copy stream_airport_hour_counts (airport_code, hour_start, flight_count) FROM '/app/stream_counts.csv' WITH (FORMAT csv, HEADER true)

COMMIT;