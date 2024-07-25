------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 1
--
-- Author: Nate Miller
-- Date: 24 May 2024
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
  -- vessel info for 'Chitose' using pipe 3 identity_core_v20231001
  ------------------------------------------------------------------------------
  chitose_vessel_info AS(
    SELECT
      ssvid,
      n_shipname,
      first_timestamp,
      last_timestamp,
      flag,
      geartype
    FROM 
    `pipe_ais_v3_published.identity_core_v20240401`
    WHERE 
      is_carrier = TRUE
      AND n_shipname = "CHITOSE"
      AND first_timestamp < end_date()
      AND last_timestamp >= start_date()
  ),

  ------------------------------------------------------------------------------
  -- append vessel_id using
  -- could combine this with the vessel_info subquery but will keep separate
  -- for clarity
  ------------------------------------------------------------------------------
  chitose_vessel_info_id AS (
    SELECT a.*, b.vessel_id
    FROM chitose_vessel_info AS a
    LEFT JOIN(
      SELECT
        ssvid,
        vessel_id,
        first_timestamp,
        last_timestamp
      FROM `pipe_ais_v3_published.vessel_info`
    ) AS b
    ON (
      a.ssvid = b.ssvid
      AND a.first_timestamp >= b.first_timestamp AND a.last_timestamp <= b.last_timestamp 
    )
  ),

  ------------------------------------------------------------------------------
  -- now we need to do the same for all fishing vessels during this period
  -- this time I've combined the two subqueries into one
  ------------------------------------------------------------------------------     
  fishing_vessel_info AS (
    SELECT 
      vessel_id,
      ssvid,
    FROM `pipe_ais_v3_published.product_vessel_info_summary_v20240401`
    WHERE
      on_fishing_list_best = TRUE
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
    FROM `pipe_ais_v3_published.encounters`
    WHERE 
      start_time < end_date()
      AND end_time >= start_date()
      AND ((vessel_1_id IN (SELECT vessel_id FROM chitose_vessel_info_id) 
          AND vessel_2_id IN (SELECT vessel_id FROM fishing_vessel_info))
      OR (vessel_2_id IN (SELECT vessel_id FROM chitose_vessel_info_id) 
          AND vessel_1_id IN (SELECT vessel_id FROM fishing_vessel_info))
          )
  ) 
 
--------------------------------------------------------------------------------
-- return encounters
--------------------------------------------------------------------------------  
SELECT *
FROM encounters