------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 1
--
-- Author: Nate Miller
-- Date: 24 May 2024
------------------------------------------------------------
-- Use temporary functions to set start and end dates
-- more consistent and less likely to make mistakes in the query by using 
-- mismatching dates
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2021-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2021-07-01");
-- 
-- How many encounter events did CHITOSE have with fishing vessels in the first 
-- six months of 2021
WITH

  ------------------------------------------------------------------------------
  -- vessel info for 'Chitose' using pipe 3 identity_core_v20231001
  ------------------------------------------------------------------------------
  chitose_vessel_info AS(
    SELECT
    vessel_id
    FROM 
    `world-fishing-827.pipe_ais_v3_published.product_vessel_info_summary_v20240401`
    WHERE 
      core_is_carrier = TRUE
      AND shipname = "CHITOSE"
      AND year = 2021
  )
  -------------------------------------------------------------------------------------------------
  -- Query encounter events from the pipe 3 encounters table
  -------------------------------------------------------------------------------------------------
  SELECT 
  *,
  TIMESTAMP_DIFF(event_end, event_start, SECOND)/3600 AS duration_hr
  FROM 
  `world-fishing-827.pipe_ais_v3_published.product_events_encounter`
  WHERE 
  vessel_id IN (SELECT vessel_id FROM chitose_vessel_info)
  AND JSON_EXTRACT(event_info, "$.vessel_classes") = '"carrier-fishing"'
  AND event_start < end_date()
  AND event_end >= start_date()