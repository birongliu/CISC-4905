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
    category
FROM workspace.flights.ingest_flights
WHERE time_position >= timestamp('{{ ti.xcom_pull(task_ids="capture_metadata")["time_position"] }}')
LIMIT 10