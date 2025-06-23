select count(1) AS trip_count from {{ source('taxi_trips', 'tripss') }}
