------------------------------------------------------------
-- Analysis training - Transshipment inspection
-- Question 2
--
-- Author: Cian Luck
-- Date: 20 June 2023
------------------------------------------------------------

-- How many loitering events by carriers occurred in IOTC in 2020?

-- Approach 2 - Query loitering
-- Note that this table doesn't include a regions_mean_position.rfmo column so
-- will have to load a shapefile of the IOTC

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
  -- create curated carrier list
  -- Remember to change the database version to the most recent version time 
  -- range of carriers should overlap with the time of encounters to ensure they 
  -- are actively transmitting during as carriers during the time of encounters
  ------------------------------------------------------------------------------
  carrier_vessels AS (
      SELECT
        mmsi as ssvid,
        flag,
        year
      FROM
        `world-fishing-827.vessel_database.carrier_vessels_byyear_v20230501`
  ),
  
  ----------------------------------------------------------
  -- Filter loitering events to those that are at least 20-nm from shore
  -- and are loitering for at least 4 hours
  -- Also filter for good segments that are not overlapping and short using gfw_research.pipe_v_segs
  -- Adjust desired timeframe
  -- restrict to loitering that started and ending within the AOI
  ----------------------------------------------------------
  loitering AS(
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
  FROM
    `pipe_production_v20201001.loitering`, iotc
  WHERE
    avg_distance_from_shore_nm >= 20
    AND loitering_hours >= 4
    AND seg_id IN (
    SELECT
      seg_id
    FROM
      `pipe_production_v20201001.research_segs`
    WHERE
      good_seg
      AND NOT overlapping_and_short)
    AND loitering_start_timestamp >= TIMESTAMP('2020-01-01')
    AND loitering_end_timestamp <= TIMESTAMP('2020-12-31')
    AND ssvid IN (
    SELECT
      ssvid
    FROM
      carrier_vessels)
    AND (ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(start_lon, start_lat))
         OR ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(end_lon, end_lat)))
    ),
      
  ----------------------------------------------------------
  -- Append carrier info
  ----------------------------------------------------------
  loitering_carriers AS (
  SELECT
    ssvid,
    seg_id,
    flag,
    loitering_start_timestamp,
    loitering_end_timestamp,
    loitering_hours,
    start_lon,
    start_lat,
    end_lon,
    end_lat
  FROM
    loitering
  LEFT JOIN
    carrier_vessels
  USING
    (ssvid)
  ORDER BY
    ssvid,
    seg_id,
    loitering_start_timestamp
    )


----------------------------------------------------------
-- Return loitering_carriers
----------------------------------------------------------
SELECT *
FROM loitering_carriers
      
      
      


