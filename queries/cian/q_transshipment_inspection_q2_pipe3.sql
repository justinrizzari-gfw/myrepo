------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 2
--
-- Author: Cian Luck
-- Date: 28 November 2023
------------------------------------------------------------

-- How many loitering events by carriers occurred in IOTC in 2020?

-- Using pipe 3 tables

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
    ST_GEOGFROMTEXT(string_field_1, make_valid => TRUE) AS polygon
  FROM `world-fishing-827.ocean_shapefiles_all_purpose.IOTC_shape_feb2021`
  ),
    
  ------------------------------------------------------------------------------
  -- create curated carrier list from pipe 3 identity_core_v
  --
  -- note that the loitering table uses ssvid (not vessel_id) so no need to
  -- append vessel_id from the vessel_info table
  ------------------------------------------------------------------------------
  carrier_vessel_info AS (
    SELECT
      ssvid,
      flag,
      n_shipname,
      first_timestamp,
      last_timestamp
    FROM `pipe_ais_v3_alpha_published.identity_core_v20231001`
    WHERE 
      is_carrier = TRUE
      AND first_timestamp < end_date()
      AND last_timestamp >= start_date()
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
      end_lat
    FROM `pipe_ais_v3_alpha_published.loitering`, iotc
    WHERE
      avg_distance_from_shore_nm >= 20
      -- AND loitering_hours >= 4
      AND loitering_hours BETWEEN 4 AND 20
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
  -- Append carrier info
  ----------------------------------------------------------
  loitering_carriers AS (
    SELECT a.*, b.* EXCEPT(ssvid, first_timestamp, last_timestamp)
    FROM loitering AS a
    LEFT JOIN (
      SELECT * 
      FROM carrier_vessel_info
    ) AS b
    ON (
      a.ssvid = b.ssvid
      AND a.loitering_start_timestamp >= b.first_timestamp AND a.loitering_end_timestamp <= b.last_timestamp 
    ) 
  )
  

----------------------------------------------------------
-- Return loitering_carriers
----------------------------------------------------------
SELECT *
FROM loitering_carriers
ORDER BY
    ssvid,
    seg_id,
    loitering_start_timestamp
      
      
      


