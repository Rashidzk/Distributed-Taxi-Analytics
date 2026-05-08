CREATE DATABASE IF NOT EXISTS taxi;

CREATE TABLE IF NOT EXISTS taxi.trips_by_hour
(
    trip_date Date,
    pickup_hour UInt8,
    trip_count UInt32,
    avg_fare Float64,
    avg_distance Float64
)
ENGINE = MergeTree
ORDER BY (trip_date, pickup_hour);