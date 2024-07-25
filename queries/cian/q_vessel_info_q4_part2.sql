--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 4
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- Is vessel considered a carrier?

--------------------------------------------------------------------------------
-- Check if vessel appears in the vessel_database.carrier_vessels_byyear_v table
--------------------------------------------------------------------------------
SELECT 
  *
FROM `world-fishing-827.vessel_database.carrier_vessels_byyear_v20230501` 
WHERE 
  year = 2018
  AND mmsi = '353154000'
  