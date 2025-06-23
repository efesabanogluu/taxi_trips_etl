select
    date(pickup_datetime) as pickup_date,
    timestamp_diff(dropoff_datetime, pickup_datetime, minute) as drive_time,
    total_amount,
    pickup_location_id
from `vertical-airway-463810-c9.dbt_test.trips_20140304`

