------------------------------------------------------------
-- Analysis training - Ports and voyages
-- Question 3
--
-- Author: Cian Luck
-- Date: 31 January 2024
------------------------------------------------------------

-- How many port events occurred in the port visit associated 
-- with the voyage that ended in Zadar on January 20, 2018? 

WITH


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
  -- identify all voyages that ended in Zadar on the date of interest
  -- note - can possibly skip this step
  ------------------------------------------------------------------------------
  -- zadar_voyages AS (
  --   SELECT
  --     trip_id,
  --     trip_end,
  --     trip_end_visit_id,
  --     trip_end_anchorage_id,
  --     trip_end_confidence
  --   FROM `pipe_ais_v3_alpha_published.voyages_c4`
  --   WHERE trip_end_anchorage_id IN (SELECT anchorage_id FROM zadar_anchorages)
  --   AND EXTRACT(DATE FROM trip_start) <= "2018-01-20"
  --   AND EXTRACT(DATE FROM trip_end) = "2018-01-20"
  -- ),

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
      EXTRACT(DATE FROM end_timestamp) = "2018-01-20"
      AND end_anchorage_id IN (SELECT anchorage_id FROM zadar_anchorages)
  )

--------------------------------------------------------------------------------
-- Count the number of events associated with each port visit
--------------------------------------------------------------------------------
SELECT 
  visit_id,
  COUNT(event_type) AS n_port_events
FROM zadar_port_events
GROUP BY visit_id

