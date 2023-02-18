from taxi_trips_etl_packages.utils.constants import TARGET_NORMALIZED_TABLE

enrichment_query = f"""
 select CASE WHEN pickup_time >= '06:00:00' AND pickup_time < '12:00:00' THEN 'Morning'
   WHEN pickup_time >= '12:00:00' AND pickup_time < '18:00:00' THEN 'Noon'
   WHEN pickup_time >= '18:00:00' AND pickup_time <= '23:59:59' THEN 'Evening'
   WHEN pickup_time < '06:00:00' THEN 'Night'  end as daypart,
    pup.pickup_datetime, pup.dropoff_datetime, pup.pickup_location_id, pup.dropoff_location_id, h3_pickup, h3_dropoff 
    from (
 select EXTRACT(TIME FROM pickup_datetime) as pickup_time, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,  `carto-os`.carto.H3_FROMGEOGPOINT((ST_CENTROID(zone_geom)),9) as h3_dropoff
     from `{TARGET_NORMALIZED_TABLE}` trips
     left join (select zone_id, zone_geom, RANK() OVER (PARTITION BY zone_id ORDER BY byte_length(ST_ASTEXT(zone_geom)) DESC) AS finish_rank
     from `premium-odyssey-377711.taxi_trips.taxi_zone_geom`) geo1 on trips.dropoff_location_id = geo1.zone_id
     where finish_rank=1
     ) doff
 inner join (
     select pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,  `carto-os`.carto.H3_FROMGEOGPOINT((ST_CENTROID(zone_geom)),9) as h3_pickup
      from `{TARGET_NORMALIZED_TABLE}` trips
      left join (select zone_id, zone_geom, RANK() OVER (PARTITION BY zone_id ORDER BY byte_length(ST_ASTEXT(zone_geom)) DESC) AS finish_rank
     from `premium-odyssey-377711.taxi_trips.taxi_zone_geom`) geo1 on trips.pickup_location_id = geo1.zone_id
      where finish_rank=1
 ) pup
 on doff.pickup_datetime = pup.pickup_datetime and  doff.dropoff_datetime = pup.dropoff_datetime 
 and  doff.pickup_location_id = pup.pickup_location_id and  doff.dropoff_location_id = pup.dropoff_location_id
 """