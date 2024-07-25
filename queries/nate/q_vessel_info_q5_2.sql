--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 5
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------

-- Is the vessel a fishing vessel? 
-- What are different ways you could determine if the vessel was a fishing vessel?

--------------------------------------------------------------------------------
-- Pull vessel info from product_vessel_info_summary_v
--------------------------------------------------------------------------------
SELECT 
ssvid,
on_fishing_list_best,
on_fishing_list_sr,
best_vessel_class,
registry_vessel_class,
inferred_vessel_class_ag,
core_geartype,
prod_shiptype,
prod_geartype,
potential_fishing,
potential_fishing_source
FROM 
`pipe_ais_v3_published.product_vessel_info_summary_v20240401`
WHERE 
ssvid = '353154000'
AND year = 2018