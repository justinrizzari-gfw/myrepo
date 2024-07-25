-- 18 Sep 2023
-- MS 
-- fishing inspection q1 
-- We may also want to know about fishing. Plot a map of the track and fishing points by the vessel with the MMSI 367650000 between March 1 2017 and March 5 2017. Are there any issues with the track?

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
    )

SELECT 
  * EXCEPT(regions)
FROM activity
LEFT JOIN vessel_database ON (ssvid = CAST(mmsi AS string))
ORDER BY timestamp