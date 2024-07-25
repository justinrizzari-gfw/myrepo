------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 5
--
-- Author: Nate Miller
-- Date: 24 May 2024
------------------------------------------------------------

-- How many loitering events that don’t overlap with encounters by the same vessel occur by carriers in IOTC in 2020?

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
      FROM 
      `world-fishing-827.pipe_ais_v3_published.product_vessel_info_summary_v20240401`
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
      
    FROM `pipe_ais_v3_published.loitering`, iotc
    WHERE
      avg_distance_from_shore_nm >= 20
      AND loitering_hours >= 4
      AND seg_id IN (
        SELECT seg_id
        FROM `pipe_ais_v3_published.segs_activity`
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
  -- Append vessel_id from carrier_vessel_info
  -- will need this to match with encounters
  ----------------------------------------------------------
  loitering_carriers AS (
    SELECT *
    FROM loitering
    JOIN (
      SELECT * 
      FROM carrier_vessel_info
    )
    USING(ssvid, year)
  ),

  ----------------------------------------------------------
  -- Identify encounters by the same vessels
  ----------------------------------------------------------
  encounters AS (
    SELECT
      encounter_id,
      start_time,
      end_time,
      mean_longitude,
      mean_latitude,
      vessel_1_id,
      vessel_2_id
    FROM `pipe_ais_v3_published.encounters`, iotc
    WHERE 
      start_time < end_date()
      AND end_time >= start_date()
      AND (vessel_1_id IN (SELECT vessel_id FROM carrier_vessel_info)
        OR vessel_2_id IN (SELECT vessel_id FROM carrier_vessel_info))
      AND (ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(start_lon, start_lat))
         OR ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(end_lon, end_lat)))
  ),

  ----------------------------------------------------------
  -- Identify loitering events that overlap with encounters
  ----------------------------------------------------------
  loitering_with_encounters AS (
    SELECT a.loitering_id
    FROM (
      SELECT *
      FROM loitering_carriers
    ) AS a
    JOIN (
      SELECT *
      FROM encounters
    ) AS b
    ON (
      (a.vessel_id = b.vessel_1_id 
      AND b.start_time < a.loitering_end_timestamp
      AND b.end_time >= a.loitering_start_timestamp) OR 
      (a.vessel_id = b.vessel_2_id 
      AND b.start_time < a.loitering_end_timestamp
      AND b.end_time >= a.loitering_start_timestamp)
    )
  ),

  ----------------------------------------------------------
  -- Use this to select for loitering events that don't
  -- overlap
  ----------------------------------------------------------
  loitering_without_encounters AS (
    SELECT *
    FROM loitering_carriers
    WHERE loitering_id NOT IN (SELECT loitering_id FROM loitering_with_encounters)
  )

----------------------------------------------------------
-- Return loitering_carriers
----------------------------------------------------------
SELECT *
FROM loitering_without_encounters
ORDER BY
    ssvid,
    loitering_start_timestamp