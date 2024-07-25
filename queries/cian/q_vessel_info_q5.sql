--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 5
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- Is the vessel a fishing vessel? 
-- What are different ways you could determine if the vessel was a fishing vessel?

--------------------------------------------------------------------------------
-- Pull vessel info from vi_ssvid_byyear_v
--------------------------------------------------------------------------------
SELECT 
  ssvid AS mmsi,
  on_fishing_list_best,
  on_fishing_list_known,
  on_fishing_list_nn,
  on_fishing_list_sr
FROM 
  `gfw_research.vi_ssvid_byyear_v20230501`
WHERE 
  ssvid = '353154000'
  AND year = 2018