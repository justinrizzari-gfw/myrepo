--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 1
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- What is the name, callsign, flag state, and imo of the vessel with mmsi 
-- 353154000 during 2018?

--------------------------------------------------------------------------------
-- Pull vessel info from vi_ssvid_byyear_v
--------------------------------------------------------------------------------
SELECT 
  ssvid AS mmsi,
  ais_identity.n_shipname_mostcommon.value AS shipname,
  ais_identity.n_imo_mostcommon.value AS imo,
  ais_identity.n_callsign_mostcommon.value AS callsign,
  best.best_flag AS flag
FROM 
  `gfw_research.vi_ssvid_byyear_v20230501`
WHERE 
  ssvid = '353154000'
  AND year = 2018
  