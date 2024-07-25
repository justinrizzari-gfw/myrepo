SELECT 
  ssvid,
  vessel_id,
  shipname,
  SUM(fishing_hours) as fishing_hours
FROM (
  SELECT
    event_id,
    vessel_id,
    JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid,
    JSON_EXTRACT_SCALAR(event_vessels, "$[0].name") as shipname,
    event_start,
    event_end,
    ROUND(TIMESTAMP_DIFF(event_end,event_start,MINUTE)/60,1) as fishing_hours
  FROM `pipe_production_v20201001.published_events_fishing`)
WHERE ssvid = {ssvid}
AND event_start <= {end_date}
AND event_end >= {start_date}
GROUP BY 1,2,3