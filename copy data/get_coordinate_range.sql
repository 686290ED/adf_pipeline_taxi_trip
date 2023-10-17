WITH
    rate AS (
        SELECT *, trip_total / trip_seconds AS second_rate
        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` 
        WHERE trip_seconds is not null and trip_seconds > 60
        AND trip_start_timestamp > '2022-01-01'
    ),
    trips_filtered AS (
        SELECT * except (second_rate), extract(year FROM trip_start_timestamp) AS yr
        FROM rate
        WHERE second_rate <= 1
    ),
    pickup_coord AS (
        SELECT distinct
            pickup_location AS location,
            pickup_latitude AS latitude,
            pickup_longitude AS longitude
        FROM trips_filtered
    ),
    dropoff_coord AS (
        SELECT distinct
            dropoff_location AS location,
            dropoff_latitude AS latitude,
            dropoff_longitude AS longitude
        FROM trips_filtered
    ),
    coord AS (
        SELECT *
        FROM pickup_coord
        union
        distinct
        SELECT *
        FROM dropoff_coord
    )

select MAX(latitude) as latitude_max, MIN(latitude) latitude_min, 
MAX(longitude) as longitude_max, MIN(longitude) as longitude_min
from coord

--latitude_max,latitude_min,longitude_max,longitude_min	
--42.021223593,41.651921576,-87.530712484-87.913624596