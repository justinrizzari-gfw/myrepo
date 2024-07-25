--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 1
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------

-- What is the name, callsign, flag state, and imo of the vessel with mmsi 
-- 353154000 during 2018?

--------------------------------------------------------------------------------
-- Pull vessel info from vi_ssvid_byyear_v NOT USING MOST COMMON
--------------------------------------------------------------------------------
SELECT 
  ssvid AS mmsi,
  ais_identity.n_shipname AS shipname,
  ais_identity.n_imo AS imo,
  ais_identity.n_callsign AS callsign,
  best.best_flag AS flag
FROM 
`world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v20240401`
WHERE 
  ssvid = '353154000'
  AND year = 2018