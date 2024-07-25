------------------------------------------------------------
-- Analysis training - Ports and voyages
-- Question 3 - bonus
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
      vessel_id,
      trip_end_anchorage_id,
      trip_end_visit_id
    FROM
    `pipe_ais_v3_alpha_published.voyages_c4`
    WHERE 
    -- ssvid IN (SELECT ssvid FROM tuna_queen_vi)
    vessel_id IN (SELECT vessel_id FROM tuna_queen_vi)
    AND trip_start <= end_date()
    AND trip_end >= start_date()
  ),

  ------------------------------------------------------------------------------
  -- create a list of anchorage_ids associated with Zadar
  ------------------------------------------------------------------------------
  zadar_anchorages AS (
    SELECT
      s2id AS anchorage_id,
      label,
      sublabel
    FROM `gfw_research.named_anchorages`
    WHERE 
      label IN ("ZADAR")
      AND sublabel IN ("ZADAR")
  ),

  ------------------------------------------------------------------------------
  -- identify all Tuna Queen voyages that ended in Zadar on the date of interest
  -- note - can possibly skip this step
  ------------------------------------------------------------------------------
  zadar_voyages AS (
    SELECT
      trip_id,
      trip_end,
      trip_end_visit_id
    FROM voyages
    WHERE trip_end_anchorage_id IN (SELECT anchorage_id FROM zadar_anchorages)
    AND EXTRACT(DATE FROM trip_start) <= "2018-01-20"
    AND EXTRACT(DATE FROM trip_end) = "2018-01-20"
  ),

  ------------------------------------------------------------------------------
  -- identify all port events associated with trips that ended in Zadar on
  -- the date of interest
  ------------------------------------------------------------------------------
  zadar_port_events AS (
    SELECT
      visit_id,
      end_anchorage_id,
      end_timestamp,
      confidence,
      event_type
    FROM `pipe_ais_v3_alpha_published.port_visits`
    LEFT JOIN UNNEST(events) AS events
    WHERE 
      EXTRACT(DATE FROM end_timestamp) = "2018-01-21"
      AND visit_id IN (SELECT trip_end_visit_id FROM zadar_voyages)
  )

--------------------------------------------------------------------------------
-- Count the number of events associated with each port visit
--------------------------------------------------------------------------------
-- SELECT *
-- FROM zadar_port_events

SELECT
  COUNT(event_type) AS n_events,
  visit_id,
  end_timestamp
FROM zadar_port_events
  GROUP BY visit_id, end_timestamp