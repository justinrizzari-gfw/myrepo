-- 18 Sep 2023
-- MS 
-- fishing inspection q2 
-- How many hours of fishing are estimated during this time? Is it different if you use the `pipe_production_v20201001.published_events_fishing` table?
-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('367650000');
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP('2017-03-01'));
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP('2017-03-05'));


WITH 

-- look at registry info and ais_identity info within this table 
vessel_database AS(
  SELECT
    DISTINCT
    uvi,
    mmsi,
    flag, 
    imo, 
    shipname_norm,
    callsign_norm, 
    geartype,
  FROM
    `world-fishing-827.vessel_database.all_vessels_20190102`
  WHERE
    CAST(mmsi AS string) = mmsi()
    AND first_timestamp <= end_date()
    AND last_timestamp >= start_date()
    ), 

activity AS (
  SELECT 
    *
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
  -- filter to date period of interest 
  _partitiontime BETWEEN start_date() AND end_date()
  -- filter to voi 
  AND ssvid = mmsi()
    ),

fishing_events AS (
 SELECT 
  * EXCEPT(regions_mean_position, event_info, event_vessels), 
  TIMESTAMP_DIFF(event_end, event_start, SECOND) / 3600 as fishing_hours,
  JSON_EXTRACT(event_info, '$.avg_duration_hrs') AS ave_fishing_hours,
  JSON_EXTRACT(event_info, '$.vessel_authorisation_status') AS authorised
  FROM 
    `world-fishing-827.pipe_production_v20201001.published_events_fishing`
  WHERE 
    seg_id IN (SELECT seg_id FROM activity)
    AND event_end <= end_date()
    AND event_start >= start_date() 
) 

SELECT * FROM fishing_events