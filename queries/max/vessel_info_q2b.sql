-- 18 Aug 2023
-- MS 
-- vessel_info q2 vi_ssvid_v
-- Is the above answer different if you pull from the vessel info table (`gfw_research.vi_ssvid_v`) 
-- versus the vessel registry table (`vessel_database.all_vessels_v`)?

-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('353154000');
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP('2018-01-01'));
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP('2019-01-01'));

WITH 

vessel_database AS(
  SELECT
    identity.ssvid,
    identity.flag, 
    identity.imo, 
    identity.n_callsign, 
    identity.n_shipname,
    is_fishing, 
    is_carrier, 
    is_bunker, 
    activity.first_timestamp,
    activity.last_timestamp 
  FROM
    `world-fishing-827.vessel_database.all_vessels_v20230701`
  LEFT JOIN UNNEST(activity) as activity
  WHERE
    identity.ssvid = mmsi()
    AND activity.first_timestamp <= end_date()
    AND activity.last_timestamp >= start_date()
    )

SELECT * FROM vessel_database