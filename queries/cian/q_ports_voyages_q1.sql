------------------------------------------------------------
-- Analysis training - Ports and voyages
-- Question 1
--
-- Author: Cian Luck
-- Date: 30 January 2024
------------------------------------------------------------

-- How many voyages did Tuna Queen have in 2018 based on a port visit confidence 
-- of 4?

-- Use temporary functions to set start and end dates
-- more consistent and less likely to make mistakes in the query by using 
-- mismatching dates
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2018-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2018-12-31");

WITH

  ------------------------------------------------------------------------------
  -- vessel info for 'Tuna Queen' using all_vessels_byyear_v2
  ------------------------------------------------------------------------------
  tuna_queen_vi AS (
    SELECT 
    ssvid,
    vessel_id,
    shipname
  FROM `pipe_production_v20201001.all_vessels_byyear_v2_v20231201` 
  WHERE shipname IN ("TUNA QUEEN") 
  AND year = 2018
  ),

  ------------------------------------------------------------------------------
  -- voyages by Tuna Queen using voyages_c4
  ------------------------------------------------------------------------------
  voyages AS (
    SELECT
      trip_id,
      trip_start,
      trip_end,
      ssvid,
      vessel_id
    FROM
    `pipe_ais_v3_alpha_published.voyages_c4`
    WHERE 
    -- ssvid IN (SELECT ssvid FROM tuna_queen_vi)
    vessel_id IN (SELECT vessel_id FROM tuna_queen_vi)
    AND trip_start <= end_date()
    AND trip_end >= start_date()
  )

--------------------------------------------------------------------------------
-- Return the count of distinct trip_id values
--------------------------------------------------------------------------------
SELECT 
  COUNT(DISTINCT trip_id) AS n_trips,
  ssvid,
  vessel_id
FROM voyages
  GROUP BY 
    ssvid,
    vessel_id
