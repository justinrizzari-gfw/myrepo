--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 2
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- Is the above answer different if you pull from the vessel info table 
-- (gfw_research.vi_ssvid_v) versus the vessel registry table 
-- (vessel_database.all_vessels_v)?


--------------------------------------------------------------------------------
-- Pull vessel info from vessel_database.all_vessels_v
--------------------------------------------------------------------------------
SELECT 
  identity.ssvid AS mmsi,
  identity.n_shipname AS shipname,
  identity.imo AS imo,
  identity.n_callsign AS callsign,
  identity.flag AS flag,
  activity.first_timestamp AS first_timestamp,
  activity.last_timestamp AS last_timestamp
FROM `world-fishing-827.vessel_database.all_vessels_v20230501` 
LEFT JOIN UNNEST(activity) as activity
WHERE matched
  AND identity.ssvid = '353154000'
  AND first_timestamp <= '2018-12-31'
  AND last_timestamp >= '2018-01-01'
  