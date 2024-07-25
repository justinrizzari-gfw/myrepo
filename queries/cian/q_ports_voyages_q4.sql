------------------------------------------------------------
-- Analysis training - Ports and voyages
-- Question 4
--
-- Author: Cian Luck
-- Date: 14 February 2024
------------------------------------------------------------

-- What is the count of port visits per port based on the end of the voyages in 2018 for Tuna Queen
-- (based on a confidence of 3 and for the ‘right’ Tuna Queen)?


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
  -- voyages by Tuna Queen using voyages_c3
  ------------------------------------------------------------------------------
  voyages AS (
    SELECT
      trip_id,
      trip_start,
      trip_end,
      trip_end_visit_id,
      trip_end_anchorage_id,
      vessel_id
    FROM
    `pipe_ais_v3_alpha_published.voyages_c3`
    WHERE 
      vessel_id IN (SELECT vessel_id FROM tuna_queen_vi)
      -- note that for this question I've filtered only on trip_end
      -- as we're interested in counting trips that ENDED in 2018
      -- (not ended or started)
      AND trip_end <= end_date()
      AND trip_end >= start_date()
            -- note that we still need to include a trip_start filter
      -- for this table
      AND trip_start >= start_date()
  ),

  ------------------------------------------------------------------------------
  -- get anchorage/port labels and append to voyages
  ------------------------------------------------------------------------------
  port_labels AS (
    SELECT
        s2id AS anchorage_id,
        label AS port_label,
        sublabel AS port_sublabel, 
        iso3 AS port_country
    FROM `gfw_research.named_anchorages`
  ),

  voyages_w_port_labels AS (
    SELECT *
    FROM voyages AS a
    LEFT JOIN(
      SELECT *
      FROM port_labels
    ) AS b
    ON (a.trip_end_anchorage_id = b.anchorage_id)
  )

--------------------------------------------------------------------------------
-- Return the count of distinct trip_end_visit_ids per port
--------------------------------------------------------------------------------
SELECT 
  COUNT(DISTINCT trip_end_visit_id) AS n_visits,
  port_label,
  port_sublabel,
  port_country
FROM voyages_w_port_labels
  GROUP BY 
    port_label,
    port_sublabel,
    port_country
ORDER BY n_visits DESC
