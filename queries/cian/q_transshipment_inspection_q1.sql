------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 1
--
-- Author: Cian Luck
-- Date: 20 June 2023
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
  -- vessel info for 'Chitose'
  --
  -- we have two options - can query vi_ssvid_byyear or vessel_database
  ------------------------------------------------------------------------------
  
  -- from vi_ssvid_by_year
  vessel_info_vi AS (
    SELECT 
    ssvid, 
    best.best_vessel_class AS best_vessel_class,
    best.best_flag AS best_flag,
    ais_identity.n_shipname_mostcommon.value AS shipname
    FROM gfw_research.vi_ssvid_byyear_v20230501
    WHERE
      -- select only vessels named 'CHITOSE'
      ais_identity.n_shipname_mostcommon.value = "CHITOSE"
      -- filter for 2021
      AND year = EXTRACT (YEAR FROM start_date())
      -- Filter out other non-carrier vessels that are called CHITOSE as well
      AND best.best_vessel_class IN (
        "reefer", "specialized_reefer", "container_reefer", 
        "fish_tender", "fish_factory", "well_boat")
  ),
  
  -- from vessel_database.all_vessels_v
  vessel_info_db AS (
    SELECT 
    -- When it's a STRUCT, ".*" can break STRUCT and return columns individually
      identity.*
    FROM vessel_database.all_vessels_v20230501
    WHERE 
      matched
      AND identity.n_shipname = "CHITOSE"
      AND is_carrier
      -- AIS activity that overlap with the time period of interest
      AND (SELECT MAX (last_timestamp) FROM UNNEST (activity)) >= start_date()
      AND (SELECT MIN (first_timestamp) FROM UNNEST (activity)) < end_date()
  ),
  
  
  -------------------------------------------------------------------------------------------------
  -- Use the published_event_encounters when you need a quicker/cleaner version
  -- of the encounter table knowing that it may be restrictive and you can't set your own filtering
  -------------------------------------------------------------------------------------------------
    encounters AS (
    SELECT 
      *
    FROM (
      SELECT 
        event_id,
        event_start,
        event_end,
        lat_mean,
        lon_mean,
        event_info,
        -- Extract MMSI number involved in the encounter events
        -- You only need to get either the first vessel ($[0]) or the second ($[1])
        -- because the table includes all combinationa (A meets with B, B meets with A
        JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid
      FROM `world-fishing-827.pipe_production_v20201001.published_events_encounters` 
      )
    WHERE 
      -- only encounters involving 'CHITOSE'
      ssvid = (SELECT ssvid FROM vessel_info_db)
      AND event_end >= start_date()
      AND event_start < end_date()
      -- Filter only events between carrier and fishing vessels
      -- Note that there are carrier-other type of events which may include 
      -- gears not fishing vessels that you may want to exclude
      AND event_info LIKE "%carrier_fishing%"
  )
  
--------------------------------------------------------------------------------
-- return encounters
--------------------------------------------------------------------------------
SELECT event_id
FROM encounters 
ORDER BY event_start
  