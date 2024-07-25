--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 4
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------

-- Is vessel considered a carrier?

--------------------------------------------------------------------------------
-- Pull vessel info from pipe_ais_v3_published.identity_core_v20240401
-- include is_carrier column
--------------------------------------------------------------------------------
SELECT 
ssvid,
n_shipname,
n_callsign,
imo,
flag,
first_timestamp,
last_timestamp,
is_carrier
FROM `world-fishing-827.pipe_ais_v3_published.identity_core_v20240401`
WHERE
ssvid = '353154000'
AND first_timestamp <= '2018-12-31'
AND last_timestamp >= '2018-01-01'
