#standardSQL
------------------------------------
-- port stops less this value (in hours) excluded and voyages merged
CREATE TEMP FUNCTION median_speed() AS (CAST(3 AS INT64));
-- min duration of the encounter
CREATE TEMP FUNCTION min_duration() AS (CAST(2 AS INT64));

---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2020-01-01'));
---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2020-12-31'));
--SET your year of interest
CREATE TEMP FUNCTION yoi() AS (CAST(2020 AS INT64));

------------------------------------
WITH
------------------------------------
-- Encounter events
------------------------------------
encounters AS (
SELECT
*
FROM
`world-fishing-827.pipe_production_v20190502.encounters`
WHERE
TIMESTAMP_DIFF(end_time, start_time, SECOND) >= (3600 * min_duration())
AND median_speed_knots <= median_speed()),
------------------------------------
-- Duplicate encounters so that we 
-- have an event for each vessel
------------------------------------
flattened_encounters AS (
SELECT
vessel_1_id AS vessel_id,
vessel_2_id AS encountered_vessel_id,
CONCAT( TO_HEX(MD5(FORMAT("encounter|%s|%s|%t|%t",
vessel_1_id,vessel_2_id, start_time, end_time))), ".1" ) AS event_id,
* EXCEPT(vessel_1_id,
vessel_2_id)
FROM
encounters
UNION ALL
SELECT
vessel_2_id AS vessel_id,
vessel_1_id AS encountered_vessel_id,
CONCAT( TO_HEX(MD5(FORMAT("encounter|%s|%s|%t|%t",
vessel_1_id,vessel_2_id, start_time, end_time))), ".2" ) AS event_id,
* EXCEPT(vessel_1_id,
vessel_2_id)
FROM
encounters ),
------------------------------------
-- Include basic vessel information 
-- on the event
------------------------------------
complete_encounter_event AS (
SELECT
encounter.*,
main_vessel.shipname.value AS main_vessel_shipname,
main_vessel.ssvid AS main_vessel_ssvid,
encountered_vessel.shipname.value AS encountered_vessel_shipname,
encountered_vessel.ssvid AS encountered_vessel_ssvid
FROM
flattened_encounters AS encounter
LEFT JOIN
`world-fishing-827.pipe_production_v20190502.vessel_info` AS main_vessel
USING
(vessel_id)
LEFT JOIN
`world-fishing-827.pipe_production_v20190502.vessel_info` AS encountered_vessel
ON
encountered_vessel_id = encountered_vessel.vessel_id ),
------------------------------------
-- Final table
------------------------------------

enc_f as(
SELECT
event_id,
'encounter' AS event_type,
vessel_id,
start_time AS event_start,
end_time AS event_end,
mean_latitude AS lat_mean,
mean_longitude AS lon_mean,
mean_latitude AS lat_min,
mean_latitude AS lat_max,
mean_longitude AS lon_min,
mean_longitude AS lon_max,
ROUND(median_distance_km,3) AS median_distance_km,
ROUND(median_speed_knots,3) AS median_speed_knots,
vessel_id AS `main_vessel_id`,
encountered_vessel_id,
main_vessel_ssvid ,
main_vessel_shipname, 
encountered_vessel_ssvid,
encountered_vessel_shipname,
ST_GEOGFROMTEXT(CONCAT('POINT (', CAST(mean_longitude AS string), ' ', CAST(mean_latitude AS string), ')')) AS event_geography
FROM
complete_encounter_event)

SELECT
*
FROM
enc_f
  WHERE
  DATE(event_start) >= minimum()
  AND DATE(event_end) <= maximum()

