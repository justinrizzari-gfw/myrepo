--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 2
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------

-- Is the above answer different if you pull from the vessel 
-- info table (`pipe_ais_v3_published.vi_ssvid_v`) vs. the product 
-- identity table (`pipe_ais_v3_published.product_vessel_info_summary_v`) vs. 
-- the vessel identity table (`pipe_ais_v3_published.identity_core_v`)?


--------------------------------------------------------------------------------
-- Pull vessel info from pipe_ais_v3_published.identity_core_v20240401
--------------------------------------------------------------------------------
SELECT 
ssvid,
n_shipname,
n_callsign,
imo,
flag,
first_timestamp,
last_timestamp
FROM `world-fishing-827.pipe_ais_v3_published.identity_core_v20240401`
WHERE
ssvid = '353154000'
AND first_timestamp <= '2018-12-31'
AND last_timestamp >= '2018-01-01'