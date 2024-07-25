------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 2
--
-- Author: Cian Luck
-- Date: 20 June 2023
------------------------------------------------------------

-- How many loitering events by carriers occurred in IOTC in 2020?

-- Approach 1 - Query published_events_loitering

SELECT
  event_id
FROM `pipe_production_v20201001.published_events_loitering`
WHERE
  -- loitering events where the median position occurred inside the IOTC region
  "IOTC" IN UNNEST (regions_mean_position.rfmo)
  AND JSON_EXTRACT_SCALAR (event_vessels, "$[0].type") = "carrier"
  AND event_end >= "2020-01-01"
  AND event_start < "2021-01-01" 
  -- consider filtering to only include loitering events within a specified range
  AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.loitering_hours") AS FLOAT64) BETWEEN 1 AND 24
  -- if you need to restrict to only high seas events
  AND ARRAY_LENGTH(regions_mean_position.eez) = 0

