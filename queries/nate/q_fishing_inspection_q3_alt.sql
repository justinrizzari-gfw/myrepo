--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 3 
--
-- Author: Nate Miller
-- Date: 24 May 2024
--------------------------------------------------------------------------------

-- What kind of vessel is it?

-- Using pipe_ais_v3_published.product_vessel_info_summary_v20240401


SELECT 
  ssvid,
  shipname,
  gfw_best_flag,
  best_vessel_class,
  registry_vessel_class,
  inferred_vessel_class_ag,
  core_geartype,
  prod_shiptype,
  prod_geartype,
  prod_geartype_source
 FROM 
 `world-fishing-827.pipe_ais_v3_published.product_vessel_info_summary_v20240401` 
 WHERE ssvid = '367650000'
    AND year = 2017