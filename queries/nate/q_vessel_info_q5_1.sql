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
-- Pull vessel info from vi_ssvid_byyear_v
--------------------------------------------------------------------------------
SELECT 
  ssvid,
  on_fishing_list_best,
  on_fishing_list_known,
  on_fishing_list_nn,
  on_fishing_list_sr
FROM 
  `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v20240401`
WHERE 
  ssvid = '353154000'
  AND year = 2018