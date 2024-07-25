-- 10 Nov 2023
-- MS 
-- transhipment inspection q2 
-- How many loitering events by carriers occurred in IOTC in 2020?


-- set variables for table query
CREATE TEMP FUNCTION start_date() AS (DATE('2020-01-01'));
CREATE TEMP FUNCTION end_date() AS (DATE('2020-12-31'));


-- get loitering events in time range 
WITH loitering AS (
  SELECT 
    * 
  FROM `world-fishing-827.pipe_ais_v3_alpha_published.loitering` 
  WHERE 
    DATE(loitering_start_timestamp) > start_date()
    AND DATE(loitering_end_timestamp) < end_date()
), 

-- find iotc transshipments by identifying segments in iotc 
-- first pull the rfmo info and segment info from research messages
-- rfmo_segs AS (
--   SELECT 
--     seg_id, 
--     JSON_EXTRACT(regions, '$.rfmo') AS rfmo
-- FROM `world-fishing-827.pipe_ais_v3_alpha_published.messages` 
-- WHERE 
--     DATE(timestamp) BETWEEN start_date() AND end_date()
-- ),

-- # only keep segments in IOTC using SEARCH 
-- iotc_segs AS(
--   SELECT
--     seg_id
--   FROM rfmo_segs
--   WHERE 
--     SEARCH(rfmo, 'IOTC') = TRUE
-- ),

-- seg approach doesn't work as segments also include activity outside of IOTC 
-- shapefile approach 

iotc AS (
  SELECT 
    geometry 
  FROM `world-fishing-827.pipe_regions_layers.IOTC_rfmo_shapefile`
),

# now we need to get a list of carriers in time range  
relevant_carriers AS (
  SELECT
    identity.ssvid,
    is_carrier,
  FROM
    `world-fishing-827.pipe_ais_v3_alpha_published.identity_all_vessels_v20231001` 
  LEFT JOIN UNNEST(activity)
  WHERE
    DATE(first_timestamp) < end_date()
    AND DATE(last_timestamp) > start_date() 
    AND is_carrier = TRUE
    )

# sub set loitering using geospatial query and carrier ssvids
SELECT 
  * EXCEPT (geometry)
FROM loitering, iotc 
WHERE 
  ST_CONTAINS(iotc.geometry, ST_GEOGPOINT(start_lon, start_lat))
  AND ssvid IN (SELECT ssvid FROM relevant_carriers)