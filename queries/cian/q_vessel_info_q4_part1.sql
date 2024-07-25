--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 4
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- Is vessel considered a carrier?

--------------------------------------------------------------------------------
-- Pull vessel info from vessel_database.all_vessels_v
-- include is_carrier column
--------------------------------------------------------------------------------
SELECT 
  identity.ssvid AS mmsi,
  identity.n_shipname AS shipname,
  identity.imo AS imo,
  identity.n_callsign AS callsign,
  identity.flag AS flag,
  activity.first_timestamp AS first_timestamp,
  activity.last_timestamp AS last_timestamp,
  -- include is_carrier
  is_carrier
  --
FROM `world-fishing-827.vessel_database.all_vessels_v20230501` 
LEFT JOIN UNNEST(activity) as activity
WHERE matched
  AND identity.ssvid = '353154000'
  AND first_timestamp <= '2018-12-31'
  AND last_timestamp >= '2018-01-01'
  