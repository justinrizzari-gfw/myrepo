--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 2
--
-- Author: Cian Luck
-- Date: 19 June 2023
--------------------------------------------------------------------------------

-- Total fishing hours different if you use the 
-- `pipe_production_v20201001.published_events_fishing` table?

WITH

  ------------------------------------------------------------------------------
  -- pull all fishing events for vessel w mmsi '367650000' between 2017-03-01 
  -- and 2017-03-05
  ------------------------------------------------------------------------------
  fishing_events AS (
    SELECT
      *,
      -- we need to calculate the timestam difference between the start and
      -- end of the event to calculate total fishing hours
      TIMESTAMP_DIFF(event_end,event_start,SECOND)/3600 as fishing_hours,
      -- use JSON_EXTRACT_ to extract the ssvid information from event_vessels
      JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid
  FROM
    `pipe_production_v20201001.published_events_fishing`
  WHERE
    -- Select data range for track using _partitiontime to make query cheaper
    event_start BETWEEN TIMESTAMP("2017-03-01") AND TIMESTAMP("2017-03-05")
    -- filter to only include the vessel of interest
    AND JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") = '367650000'
  )
  
--------------------------------------------------------------------------------
-- return total fishing hours
--------------------------------------------------------------------------------
SELECT
  ssvid,
  SUM(fishing_hours) AS fishing_hours
FROM fishing_events
  GROUP BY ssvid