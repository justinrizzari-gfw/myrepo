--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 4
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------

-- Is vessel considered a carrier?

--------------------------------------------------------------------------------
-- Pull vessel info from pipe_ais_v3_published.product_vessel_info_summary_v20240401
-- include core_is_carrier column
--------------------------------------------------------------------------------
SELECT 
vessel_id,
  ssvid,
  year,
  shipname,
  imo,
  callsign,
  mmsi_flag,
  gfw_best_flag,
  core_flag,
  core_is_carrier
FROM 
`pipe_ais_v3_published.product_vessel_info_summary_v20240401`
WHERE 
ssvid = '353154000'
AND year = 2018