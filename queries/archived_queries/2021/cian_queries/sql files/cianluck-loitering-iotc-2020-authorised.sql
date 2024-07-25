-----------------------------------------------
-- Query: How many loitering events by carriers 
-- occurred in IOTC in 2020 while authorised
-----------------------------------------------

-- Cian Luck, Updated 24 Aug 2021

-- Based on example query loitering_carrier_basic.sql
-- This query finds all carrier vessels with segments that are loitering for at least 4 hours
-- and are at least 20-nm from shore


---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2020-01-01'));
---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2020-12-31'));
--SET your year of interest
CREATE TEMP FUNCTION yoi() AS (CAST(2020 AS INT64));



WITH
-----------------------------------------------
-- Restrict to carrier vessels using the vessel database
-----------------------------------------------
  carrier_vessels AS (
  SELECT
    identity.ssvid AS ssvid,
    identity.n_shipname AS shipname,
    identity.flag AS flag_state,
  FROM
    `vessel_database.all_vessels_v20210601`
  LEFT JOIN
    UNNEST(registry)
  LEFT JOIN
    UNNEST(activity)
  WHERE
    is_carrier
    AND confidence >= 3 ),

-----------------------------------------------
-- Load shapefile of IOTC
-----------------------------------------------
    iotc AS (
    SELECT
    -- note: had to add make_valid => TRUE as seems to be a problem with the shapefile
        ST_GEOGFROMTEXT(string_field_1, make_valid => TRUE) AS polygon 
    FROM
        `world-fishing-827.ocean_shapefiles_all_purpose.IOTC_shape_feb2021`
    ), 

-----------------------------------------------    
-- Filter loitering events to those that are at least 20-nm from shore
-- and are loitering for at least 4 hours
-- Also filter for good segments that are not overlapping and short using gfw_research.pipe_v_segs
-- Adjust desired timeframe
-- Filter only locations within IOTC
-----------------------------------------------
  loitering AS(
  SELECT
    ssvid,
    seg_id,
    loitering_start_timestamp,
    loitering_end_timestamp,
    loitering_hours,
  FROM
    `pipe_production_v20201001.loitering`, iotc
  WHERE
    avg_distance_from_shore_nm >= 20
    AND loitering_hours >= 4
    AND seg_id IN (
    SELECT
      seg_id
    FROM
      gfw_research.pipe_v20201001_segs
    WHERE
      good_seg
      AND NOT overlapping_and_short)
    AND loitering_start_timestamp >= TIMESTAMP('2020-01-01')
    AND loitering_end_timestamp <= TIMESTAMP('2020-12-31')
    AND ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(start_lon, start_lat))
    AND ssvid IN (
    SELECT
      ssvid
    FROM
      carrier_vessels)),

-----------------------------------------------
-- loitering events
-----------------------------------------------     
    loitering_events AS (
    SELECT
        ssvid,
        seg_id,
        shipname,
        flag_state,
        loitering_start_timestamp,
        loitering_end_timestamp,
        loitering_hours,
    FROM
        loitering
    LEFT JOIN
        carrier_vessels
    USING
        (ssvid)
    ORDER BY
        ssvid,
        seg_id,
        loitering_start_timestamp),


-----------------------------------------------
-- which vessels were authorised by IOTC in 2020
-----------------------------------------------  
    iotc_auth_info AS (
    SELECT
    auth_ssvid,
    registry_authorized_to,
    registry_authorized_from,
    reg,
    FROM(
    SELECT
    identity.ssvid as auth_ssvid,
    authorized_to as registry_authorized_to,
    authorized_from as registry_authorized_from,
    is_active as registry_active,
    SAFE_CAST( SPLIT(list_uvi, '-')[OFFSET(0)] AS string) AS reg,
        scraped,
        list_uvi,
    geartype_original as registry_gear,
    feature_gear,
    last_modified
    FROM
    `vessel_database.all_vessels_v20210601`
    LEFT JOIN UNNEST(registry)
    LEFT JOIN UNNEST(activity)
    LEFT JOIN UNNEST(feature.geartype) as feature_gear
    WHERE
    DATE(last_timestamp) >= minimum()
    AND
    DATE(first_timestamp) <= maximum()
    )
    WHERE
    (EXTRACT (YEAR from registry_authorized_to) = yoi()
    OR
    EXTRACT (YEAR from registry_authorized_from) = yoi())
    AND reg = 'IOTC'
    GROUP BY
    auth_ssvid,
    registry_authorized_to,
    registry_authorized_from,
    reg
    ),

-----------------------------------------------
-- which loitering events occurred during authorised periods
----------------------------------------------- 
    loitering_auth AS(
        SELECT * 
        FROM (
        SELECT
        *
        FROM
        loitering_events) a
        JOIN (
        SELECT *
        FROM
        iotc_auth_info ) b
        ON a.ssvid = b.auth_ssvid 
            AND(a.loitering_start_timestamp >= b.registry_authorized_from 
                AND a.loitering_end_timestamp <= b.registry_authorized_to))



-----------------------------------------------
-- Return loitering_auth
-----------------------------------------------  
SELECT *
FROM loitering_auth 