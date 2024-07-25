--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 3 
--
-- Author: Cian Luck
-- Date: 19 June 2023
--------------------------------------------------------------------------------

-- What kind of vessel is it?

-- Using all_vessels_byyear_v2


SELECT 
  ssvid,
  shipname,
  flag,
  best_vessel_class,
  registry_vessel_class,
  inferred_vessel_class_ag,
  identity_core_geartype,
  prod_shiptype,
  prod_geartype,
  prod_geartype_source
 FROM `world-fishing-827.scratch_willa_ttl100.all_vessels_byyear_v2_v20230501` 
 WHERE ssvid = '367650000'
    AND year = 2017