-- 10 Nov 2023
-- MS 
-- transhipment inspection q3 
-- How many of the carriers from question 2 were authorized by IOTC or CCSBT during the time of their loitering events?


-- 10 Nov 2023
-- MS 
-- transhipment inspection q3 
-- How many of the carriers from question 2 were authorized by IOTC or CCSBT during the time of their loitering events?


-- set variables for table query
CREATE TEMP FUNCTION start_date() AS (DATE('2020-01-01'));
CREATE TEMP FUNCTION end_date() AS (DATE('2020-12-31'));

# now we need to get a list of carriers in time range  
WITH relevant_carriers AS (
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
    ), 

auths AS (
  SELECT 
    *
  FROM `world-fishing-827.pipe_ais_v3_alpha_published.identity_authorization_v20231001`
  WHERE 
    DATE(authorized_from) < end_date()
    AND DATE(authorized_to) > start_date()
    AND ssvid IN (SELECT ssvid FROM relevant_carriers)
    AND source_code IN ('CCSBT', 'IOTC')
), 

-- get loitering events in time range 
loitering AS (
  SELECT 
    * 
  FROM `world-fishing-827.pipe_ais_v3_alpha_published.loitering` 
  WHERE 
    DATE(loitering_start_timestamp) > start_date()
    AND DATE(loitering_end_timestamp) < end_date()
), 

iotc AS (
  SELECT 
    geometry 
  FROM `world-fishing-827.pipe_regions_layers.IOTC_rfmo_shapefile`
),

iotc_loit AS (
  SELECT 
    * EXCEPT (geometry)
  FROM loitering, iotc 
  WHERE 
    ST_CONTAINS(iotc.geometry, ST_GEOGPOINT(start_lon, start_lat))
    AND ssvid IN (SELECT ssvid FROM relevant_carriers)
) 

# authorised loitering?
SELECT 
  * 
FROM iotc_loit l
INNER JOIN auths a ON(
  l.ssvid = a.ssvid 
  AND l.loitering_start_timestamp > a.authorized_from
  AND l.loitering_end_timestamp < a.authorized_to
)
