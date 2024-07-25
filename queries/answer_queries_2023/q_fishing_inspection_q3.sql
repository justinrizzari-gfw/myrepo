--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 3
--
-- Author: Cian Luck
-- Date: 19 June 2023
--------------------------------------------------------------------------------

-- What kind of vessel is it?

WITH

  ------------------------------------------------------------------------------
  -- pull vessel info from vi_ssvid_byyear_
  ------------------------------------------------------------------------------
  vessel_info AS (
    SELECT
      ssvid,
      ais_identity.n_shipname_mostcommon.value as shipname,
      ais_identity.n_imo_mostcommon.value as imo,
      ais_identity.n_callsign_mostcommon.value as callsign,
      best.best_flag as flag,
      best.best_vessel_class
    FROM
      `gfw_research.vi_ssvid_byyear_v20230501`
    WHERE
    ssvid = '367650000'
    AND year = 2017
  )
  
--------------------------------------------------------------------------------
-- return vessel_info
--------------------------------------------------------------------------------
SELECT *
FROM vessel_info