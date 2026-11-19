BEGIN;

-- Table for first query
-- What are the 10 busiest origin airports and the 10 busiest destination airports?
CREATE TABLE IF NOT EXISTS batch_airport_counts (
    airport_role text NOT NULL CHECK (airport_role IN ('origin', 'destination')),
    airport_code text NOT NULL,
    flight_count bigint NOT NULL CHECK (flight_count >= 0),
    PRIMARY KEY (airport_role, airport_code)
);

-- Table for second query
-- How many flights were recorded in each summer from 2019 to 2022?
CREATE TABLE IF NOT EXISTS batch_year_counts (
    flight_year integer PRIMARY KEY,
    flight_count bigint NOT NULL CHECK (flight_count >= 0)
);


-- Table for third query
-- Which five airport-hour combinations had the most departures during the replay?
CREATE TABLE IF NOT EXISTS stream_airport_hour_counts (
    airport_code text NOT NULL,
    hour_start timestamp NOT NULL,
    flight_count bigint NOT NULL CHECK (flight_count >= 0),
    PRIMARY KEY (airport_code, hour_start)
);

COMMIT;