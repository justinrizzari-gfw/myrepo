------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 1
--
-- Author: Cian Luck
-- Date: 28 November 2023
------------------------------------------------------------

-- How many encounter events did CHITOSE have with fishing vessels in the first 
-- six months of 2021

-- Use temporary functions to set start and end dates
-- more consistent and less likely to make mistakes in the query by using 
-- mismatching dates
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2021-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2021-07-01");

WITH

  ------------------------------------------------------------------------------
  -- vessel info for 'Chitose' using product_vessel_info_summary_v20240401
  ------------------------------------------------------------------------------
  chitose_vessel_info AS (
  SELECT 
    ssvid,
    vessel_id
  FROM `pipe_ais_v3_published.product_vessel_info_summary_v20240401` 
  WHERE 
  shipname IN ("CHITOSE") 
  AND core_is_carrier = TRUE
  AND year = 2021
  ),

  ------------------------------------------------------------------------------
  -- now we need to do the same for all fishing vessels during this period
  ------------------------------------------------------------------------------     
  fishing_vessel_info AS (
    SELECT 
      vessel_id,
      ssvid,
    FROM `pipe_ais_v3_published.product_vessel_info_summary_v20240401`
    WHERE
      prod_shiptype = "fishing"
      AND year = 2021
  ),

  -------------------------------------------------------------------------------------------------
  -- Query encounter events from the pipe 3 encounters table
  -------------------------------------------------------------------------------------------------
  encounters AS (
    SELECT
      encounter_id,
      start_time,
      end_time,
      mean_longitude,
      mean_latitude,
      DATETIME_DIFF(end_time, start_time, SECOND)/3600 AS duration_hours
    FROM `pipe_ais_v3_published.encounters`
    WHERE 
      start_time < end_date()
      AND end_time >= start_date()
      AND ((vessel_1_id IN (SELECT vessel_id FROM chitose_vessel_info) 
          AND vessel_2_id IN (SELECT vessel_id FROM fishing_vessel_info))
      OR (vessel_2_id IN (SELECT vessel_id FROM chitose_vessel_info) 
          AND vessel_1_id IN (SELECT vessel_id FROM fishing_vessel_info))
          )
  ) 
 
--------------------------------------------------------------------------------
-- return encounters
--------------------------------------------------------------------------------  
SELECT *
FROM encounters
WHERE duration_hours BETWEEN 4 and 47