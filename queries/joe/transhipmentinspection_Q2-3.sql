# This query finds all carrier vessels with loitering events in IOTC in 2020

## set dates of interest
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP('2020-01-01 00:00:00 UTC'));
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP('2020-12-31 23:59:59 UTC'));

WITH
--------------------------------------------------------------
-- first get good carriers within time range
--------------------------------------------------------------
-- using identity core
  -- high_conf_carriers AS (
  --   SELECT DISTINCT 
  --     ssvid,
  --     'carrier' AS vessel_class,
  --     geartype,
  --     first_timestamp,
  --     last_timestamp
  --   FROM 
  --     `pipe_ais_v3_alpha_published.identity_core_v20231001` -- update to latest v    
  --   WHERE
  --     TIMESTAMP(first_timestamp) <= end_date() AND
  --     TIMESTAMP(last_timestamp) >= start_date() AND
  --     is_carrier = TRUE AND
  --     geartype IN ("reefer","specialized_reefer") AND
  --     n_shipname IS NOT NULL AND
  --     flag IS NOT NULL
  -- ),

-- using allvesselsbyyearv2
 high_conf_carriers AS (
    SELECT DISTINCT 
      ssvid,
      year,
      best_vessel_class
    FROM 
      `pipe_production_v20201001.all_vessels_byyear_v2` -- update to latest v    
    WHERE
      year = 2021 AND
      core_is_carrier = TRUE AND
      best_vessel_class IN ("reefer","specialized_reefer") AND
      shipname IS NOT NULL AND
      gfw_best_flag IS NOT NULL AND
      noisy_vessel = FALSE
  ),

## loitering with vessel information extracted from event_vessels field
# note you get 90 more events if use ssvid vs vessel_id
loit AS (
  SELECT
    event_id,
    event_type,
    vessel_id,
    event_start,
    event_end,
    ## extract information on vessel ssvid and vessel type
    JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid,
    JSON_EXTRACT_SCALAR(event_vessels, "$[0].type") as vessel_type,
    ## pull out event regions
    rfmo,
    regions_mean_position.eez as eez,
    regions_mean_position.major_fao as major_fao,
    regions_mean_position.high_seas as high_seas,
    -- event_info,
    -- event_vessels,
    FROM `pipe_production_v20201001.published_events_loitering_v2_v20240123`,
    UNNEST (regions_mean_position.rfmo) as rfmo 
    WHERE 
      event_start BETWEEN start_date() AND end_date()
),

iotc AS (
  SELECT * 
  FROM loit
  WHERE ssvid IN ( 
    SELECT ssvid
    FROM high_conf_carriers)
    AND rfmo = 'IOTC'
),

auth AS (
  SELECT * 
  FROM iotc
  LEFT JOIN
  (SELECT
    ssvid,
    n_shipname,
    flag,
    authorized_from,
    authorized_to,
    source_code
  FROM `pipe_ais_v3_alpha_published.identity_authorization_v20231001`
  WHERE source_code IN ("IOTC", "CCSBT")) auth
  USING (ssvid)
),

-- label each event as authorized if occured within auth period of vessel
auth2 AS(
  SELECT
  *,
  CASE WHEN event_start BETWEEN authorized_from AND authorized_to THEN 1
  ELSE 0 END authorized
  FROM auth
),

-- merge events summing over auth to give indicator if auth or not
event_auth AS(
  SELECT 
    event_id,
    ssvid,
    event_start,
    event_end,
    rfmo,
    CASE WHEN sum(authorized) >= 1 THEN 1 ELSE 0 END authorized
  FROM auth2
  GROUP BY  
    event_id,
    ssvid,
    event_start,
    event_end,
    rfmo
),

vessels AS(
SELECT
  -- event_id,
  ssvid,
  authorized,
  -- count(*)
FROM event_auth
GROUP BY ssvid, authorized
)

SELECT ssvid, count(*)
FROM vessels
group by ssvid
/*
*/