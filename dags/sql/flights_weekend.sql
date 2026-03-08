SELECT 
    time_ingest,
    icao24,
    callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    geo_altitude,
    on_ground,
    velocity,
    category,
    dayofweek(time_position) as day_of_week
FROM workspace.flights.ingest_flights
WHERE time_position >= timestamp('{{ ti.xcom_pull(task_ids="capture_metadata")["time_position"] }}')
    AND dayofweek(time_position) IN (1, 7)  -- Sunday(1) and Saturday(7)
LIMIT 10
