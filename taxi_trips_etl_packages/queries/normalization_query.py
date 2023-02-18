from taxi_trips_etl_packages.utils.constants import TARGET_SAMPLE_TABLE

normalization_query = f"""
select * from `{TARGET_SAMPLE_TABLE}` where 
pickup_datetime != dropoff_datetime and                         
passenger_count != 0 and passenger_count is not Null and
trip_distance != 0 and trip_distance is not Null and
fare_amount != 0
"""

# trip time should be bigger than 0.
# passenger count should be bigger than 0.
# trip distance should be bigger than 0.
# fare amount should be bigger than 0.
