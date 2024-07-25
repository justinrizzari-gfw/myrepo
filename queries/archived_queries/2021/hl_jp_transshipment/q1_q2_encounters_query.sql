---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2020-01-01'));
---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2020-12-31'));
--SET your year of interest
CREATE TEMP FUNCTION yoi() AS (CAST(2020 AS INT64));
####
-- Retrieve initial encounter data, specifing time range and lat/lon
-- JSON_EXTRACT is used to seperate the listed event_info data of interest into separate columns
WITH
encounters AS (
  SELECT
  event_id,
  vessel_id,
  event_start,
  event_end,
  lat_mean,
  lon_mean,
  JSON_EXTRACT(event_info,
               "$.median_distance_km") AS median_distance_km,
  JSON_EXTRACT(event_info,
               "$.median_speed_knots") AS median_speed_knots,
  SPLIT(event_id, ".")[ORDINAL(1)] AS event,
  CAST (event_start AS DATE) event_date,
  EXTRACT(YEAR FROM event_start) AS year
  FROM
  `world-fishing-827.pipe_production_v20190502.published_events_encounters`
  WHERE
  DATE(event_start) >= minimum()
  AND DATE(event_end) <= maximum()
  ),
  #####
--grab information on ssvid corresponding to vessel_id
ssvid_map AS (
  SELECT
  vessel_id,
  ssvid
  FROM
  `world-fishing-827.pipe_production_v20190502.vessel_info`),
###
# encounters with ssvid
###
-- Join the encounters data with the ssvid data on the same vessel_id and event day to ensure correct SSVID
encounter_ssvid AS (
  SELECT * EXCEPT(vessel_id)
  FROM (
    SELECT
    *
      FROM
    encounters) a
  JOIN (
    SELECT *
      FROM
    ssvid_map) b
  ON a.vessel_id = b.vessel_id),
  
  #####
---create curated carrier list
---Remember to change the database version from _v20200801 based on the most recent version
--time range of carriers should overlap with the time of encounters to ensure they are actively transmitting during 
-- during the time of encounters
carrier_vessels AS (
  SELECT
  identity.ssvid AS carrier_ssvid,
  identity.imo AS carrier_imo_ais,
  identity.n_shipname AS carrier_shipname_ais,
  identity.n_callsign AS carrier_callsign_ais,
  identity.flag AS carrier_flag,
  feature_gear as carrier_label,
  first_timestamp AS carrier_first_timestamp,
  last_timestamp AS carrier_last_timestamp,
  FROM
 ---HANNAH note: Always make sure to use most recent version of v.database that has been approved for analyst use
  `world-fishing-827.vessel_database.all_vessels_v20201201`
  LEFT JOIN UNNEST(registry)
  LEFT JOIN UNNEST(activity)
  LEFT JOIN UNNEST(feature.geartype) as feature_gear
 ----HANNAH note: The CVP uses is_carrier and matched = TRUE to restrict list of carriers, this means the vessel must be considered a carrier and must have -----------matched registry records. However, in the example query I use is_carrier and confidence >=3 because that means that the vessel must be considered a
 ---carrier and have been manually reviewed or on a high confidence registry (like IMO or an RFMO). Once again, this is a choice up to the analyst, but important
 --to consider the options
 
  WHERE is_carrier AND
  identity.ssvid NOT IN ('111111111','0','888888888','416202700')
  AND
  DATE(first_timestamp) <= maximum()
  AND DATE(last_timestamp) >= minimum()
  GROUP BY 1,2,3,4,5,6,7,8),
###
--Identify encounters with carriers
--time range of carriers should overlap with the time of encounters to ensure they are actively transmitting during 
--as carriers during the time of encounters
encounters_carriers AS(
  SELECT
  *
    FROM (
      SELECT
      *
        FROM
      encounter_ssvid)a
  JOIN (
    SELECT
    *
      FROM
    carrier_vessels)b
  ON
  a.ssvid=SAFE_CAST(b.carrier_ssvid AS STRING)
  AND a.event_start BETWEEN b.carrier_first_timestamp
  AND b.carrier_last_timestamp
  AND a.event_end BETWEEN b.carrier_first_timestamp
  AND b.carrier_last_timestamp),
####
--Join vessel the carrier encountered
all_encounters as (
  SELECT
  carrier_ssvid,
  carrier_label,
  neighbor_ssvid,
  event_start,
  event_end,
  lat_mean as mean_lat,
  lon_mean as mean_lon,
  median_distance_km,
  median_speed_knots,
  (TIMESTAMP_DIFF(event_end,event_start,minute)/60) event_duration_hr,
  a.event AS event,
  event_date
  FROM
  (
    SELECT
    *
      FROM
    encounters_carriers) a
  JOIN
  (SELECT
    ssvid AS neighbor_ssvid,
    event
    FROM
    encounter_ssvid) b
  ON a.event = b.event
  WHERE carrier_ssvid != neighbor_ssvid
  GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12),
######
--add vessel info to the carriers
carrier_vessel_info as(
  SELECT
  *
    FROM
  (SELECT
    *
      FROM
    all_encounters)a
  LEFT JOIN(
    SELECT
    ssvid as ssvid,
    ais_identity.n_shipname_mostcommon.value AS carrier_shipname, 
    ais_identity.n_callsign_mostcommon.value AS carrier_callsign,
    ais_identity.n_imo_mostcommon.value as carrier_imo,
    activity.first_timestamp as first_timestamp,
    activity.last_timestamp as last_timestamp,
    IF(best.best_flag is NULL, ais_identity.flag_mmsi, best.best_flag) AS carrier_flag
    FROM
    ---HANNAH note: Always used most recent approved version of this table
    gfw_research.vi_ssvid_byyear_v20201209
    WHERE
    year = yoi())b
  ON
  SAFE_CAST(a.carrier_ssvid as int64)=SAFE_CAST(b.ssvid as int64)
  AND
  a.event_start>=b.first_timestamp
  AND
  a.event_end<=b.last_timestamp),
####
--Add the vessel info to the neighbor vessel
neighbor_vessel_info as(
  SELECT
  *
    FROM
  (SELECT
    *
      FROM
    carrier_vessel_info)a
  LEFT JOIN(
    SELECT
    ssvid,
    ais_identity.n_shipname_mostcommon.value  as neighbor_shipname,
    ais_identity.n_imo_mostcommon.value as neighbor_imo,
    ais_identity.n_callsign_mostcommon.value   as neighbor_callsign,
    IF(best.best_flag is NULL, ais_identity.flag_mmsi, best.best_flag)  as neighbor_flag,
    IF(best.best_vessel_class IS NULL, inferred.inferred_vessel_class, best.best_vessel_class) AS neighbor_label,
    activity.first_timestamp as neighbor_first_timestamp,
    activity.last_timestamp as neighbor_last_timestamp,
    on_fishing_list_best
    FROM
    ---HANNAH note: Here again, need most recent version
    `gfw_research.vi_ssvid_byyear_v20201209` 
    WHERE
    year = yoi())b
  ON
  SAFE_CAST(a.neighbor_ssvid as int64)=SAFE_CAST(b.ssvid as int64)
  AND
  a.event_start>=b.neighbor_first_timestamp
  AND
  a.event_end<=b.neighbor_last_timestamp),

--HANNAH NOTE: added this query below
####Get fishing list vessels, this list is a slighty more constricted version of vessels that are on_fishing_list_best (see details of table in BQ)
####Depending on how 'strict' you want to be in the list of vessels you identify as fishing, you can use this (like the CVP) or you when use the vessel_info
###table where on_fishing_list_best = TRUE
fishing_v as (
SELECT
*
FROM
`gfw_research.fishing_vessels_ssvid_v20201209` 
WHERE
year = 2020),

#####
--Clean up the data to the columns we are interested in, and to only carrier-fishing encounters
--and group data to remove any possible duplications generated in the process of creating data
cv_all as(
  SELECT
  carrier_ssvid,
  carrier_shipname,
  carrier_imo,
  carrier_callsign,
  carrier_label,
  carrier_flag,
  neighbor_ssvid,
  neighbor_shipname,
  neighbor_imo,
  neighbor_callsign,
  neighbor_flag,
  neighbor_label,
  event_start,
  event_end,
  mean_lat,
  mean_lon,
  median_distance_km,
  median_speed_knots,
  event_duration_hr
  FROM
  neighbor_vessel_info
  ---HANNAH NOTE: changed where fishing vessels are pulled from
  WHERE
  neighbor_ssvid IN (
  SELECT
  ssvid
  FROM
  fishing_v)
  GROUP BY
  carrier_ssvid,
  carrier_shipname,
  carrier_imo,
  carrier_callsign,
  carrier_label,
  carrier_flag,
  neighbor_ssvid,
  neighbor_shipname,
  neighbor_imo,
  neighbor_callsign,
  neighbor_flag,
  neighbor_label,
  event_start,
  event_end,
  mean_lat,
  mean_lon,
  median_distance_km,
  median_speed_knots,
  event_duration_hr),
### add rfmo polygon shapefile of interest - in this case NPFC
rfmo as(
  SELECT
  st_GeogFromText(string_field_1) AS npfc
  FROM
  `ocean_shapefiles_all_purpose.NPFC_shape`),
--Update encounters to only those that are within the NPFC shapefile
encs_upd as(
  SELECT
  carrier_ssvid,
  carrier_shipname,
  carrier_imo,
  carrier_callsign,
  carrier_label,
  carrier_flag,
  neighbor_ssvid,
  neighbor_shipname,
  neighbor_imo,
  neighbor_callsign,
  neighbor_flag,
  neighbor_label,
  event_start,
  event_end,
  mean_lat,
  mean_lon,
  median_distance_km,
  median_speed_knots,
  event_duration_hr,
  npfc_loc
  FROM(
    SELECT
    *,
    ST_CONTAINS(npfc,
                ST_GeogPoint(mean_lon,
                             mean_lat)) as npfc_loc
    FROM(
      SELECT *
        FROM cv_all)a
    CROSS JOIN(
      SELECT
      *
        FROM
      rfmo)b)
  WHERE npfc_loc = TRUE
)

SELECT
*
  FROM
encs_upd
WHERE
carrier_flag = 'PAN'
