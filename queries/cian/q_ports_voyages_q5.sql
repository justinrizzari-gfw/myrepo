------------------------------------------------------------
-- Analysis training - Ports and voyages
-- Question 5
--
-- Author: Cian Luck
-- Date: 14 Feburary 2024
------------------------------------------------------------

-- What ports were visited after loitering events, that were at least 4 hours and less than or equal to 24 hours, 
-- greater than avg 20 nm from shore, and less than 2 knots, by carriers in IOTC in 2020 (based on confidence of 4)? 

-- What is the number of port visits at each of these ports?

-- Use temporary functions to set start and end dates
-- more consistent and less likely to make mistakes in the query by using 
-- mismatching dates
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2020-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2020-12-31");

WITH

	------------------------------------------------------------------------------
  -- Load shapefile of IOTC
  ------------------------------------------------------------------------------
  iotc AS (
  SELECT
     id,
     ST_UNION_AGG(geo) AS polygon
  FROM `world-fishing-827.pipe_regions_layers.event_regions` 
  WHERE 
    layer = 'rfmo' 
    AND id = 'IOTC'
    GROUP BY 1
  ),
    
  ------------------------------------------------------------------------------
  -- create curated carrier list from all_vessels_byyear_v2
  ------------------------------------------------------------------------------
  carrier_vessel_info AS (
      SELECT 
        ssvid,
        vessel_id,
        year
      FROM `pipe_production_v20201001.all_vessels_byyear_v2_v20231201`
      WHERE
        prod_shiptype = "carrier"
        AND year = 2020
    ),
  
  ----------------------------------------------------------
  -- Filter loitering events to those that are at least 20-nm from shore
  -- and are loitering for at least 4 hours
  -- Also filter for good segments that are not overlapping and short using gfw_research.pipe_v_segs
  -- Adjust desired timeframe
  -- restrict to loitering that started and ending within the AOI
  ----------------------------------------------------------
  loitering AS (
    SELECT
      ssvid,
      seg_id,
      loitering_start_timestamp,
      loitering_end_timestamp,
      loitering_hours,
      start_lon,
      start_lat,
      end_lon,
      end_lat,
      TO_HEX(
      MD5 (
        format("%s|%t|%t",
        ssvid,
        loitering_start_timestamp, loitering_end_timestamp))) AS loitering_id,
      EXTRACT(year FROM loitering_start_timestamp) AS year
      
    FROM `pipe_ais_v3_alpha_published.loitering`, iotc
    WHERE
      avg_distance_from_shore_nm >= 20
      AND loitering_hours BETWEEN 4 AND 24
      AND avg_speed_knots < 2
      -- AND avg_distance_from_shore_nm > 20
      AND seg_id IN (
        SELECT seg_id
        FROM `pipe_ais_v3_alpha_published.segs_activity`
        WHERE good_seg
        AND NOT overlapping_and_short
      )
      AND loitering_start_timestamp >= start_date()
      AND loitering_end_timestamp <= end_date()
      AND ssvid IN (SELECT ssvid FROM carrier_vessel_info)
      AND (ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(start_lon, start_lat))
         OR ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(end_lon, end_lat)))
  ),

  ----------------------------------------------------------
  -- Append vessel_id to loitering using seg_id
  -- this matches better with port and voyages
  ---------------------------------------------------------- 
  --loitering_carriers AS (
  --   SELECT *
  --   FROM loitering
  --   JOIN (
  --     SELECT * 
  --     FROM carrier_vessel_info
  --   )
  --   USING(ssvid, year)
  -- ),

  -- loitering_carriers AS (
  --   SELECT a.*, b.vessel_id
  --   FROM loitering AS a
  --   LEFT JOIN (
  --     SELECT
  --       ssvid,
  --       vessel_id,
  --       first_timestamp,
  --       last_timestamp
  --     FROM pipe_ais_v3_alpha_published.vessel_info
  --   ) AS b
  --   ON (a.ssvid = b.ssvid
  --       AND a.loitering_start_timestamp <= b.last_timestamp
  --       AND a.loitering_end_timestamp >= b.first_timestamp)
  -- ), 

    loitering_carriers AS (
    SELECT a.*, b.vessel_id
    FROM loitering AS a
    LEFT JOIN (
      SELECT
        vessel_id,
        seg_id
      FROM pipe_ais_v3_alpha_published.segment_info
    ) AS b
    USING(seg_id)
  ), 


  ----------------------------------------------------------
  -- Need to find out the trip_id associated with each of 
  -- these loitering events
  ----------------------------------------------------------
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
      vessel_id IN (SELECT vessel_id FROM loitering_carriers)
      AND trip_start <= end_date()
      AND trip_end >= start_date()
  ),

  loitering_voyages AS (
    SELECT 
      a.*, 
      b.trip_id, 
      b.trip_end_visit_id, 
      b.trip_end_anchorage_id
    FROM loitering_carriers AS a
    LEFT JOIN (
      SELECT *
      FROM voyages
    ) AS b
    ON (
      a.vessel_id = b.vessel_id
      AND a.loitering_start_timestamp <= b.trip_end
      AND a.loitering_end_timestamp >= b.trip_start
    )
  ),

  ----------------------------------------------------------
  -- Now we have the trip information associated with each 
  -- loitering event
  -- 
  -- Next we need to append the port information
  ----------------------------------------------------------
  port_labels AS (
    SELECT
        s2id AS anchorage_id,
        label AS port_label,
        sublabel AS port_sublabel, 
        iso3 AS port_country
    FROM `gfw_research.named_anchorages`
  ),

  loitering_voyages_w_port_labels AS (
    SELECT *
    FROM loitering_voyages AS a
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
FROM loitering_voyages_w_port_labels
  GROUP BY 
    port_label,
    port_sublabel,
    port_country
ORDER BY n_visits DESC