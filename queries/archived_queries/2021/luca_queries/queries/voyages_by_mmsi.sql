WITH
trip_ids AS (
  SELECT
  *,
  TIMESTAMP_DIFF(next_voyage_start, trip_end, SECOND)/3600 port_stop_duration_hr
  FROM (
  SELECT
  *,
  LEAD(trip_start, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_start
  FROM (
  SELECT
  *
  FROM (
  SELECT
  *
  FROM
  --This voyages table includes all voyages with port visits that have a confidence of >=2
  `pipe_production_v20201001.proto_voyages_c4`
  )))),


--####
--identify if the current, previous, or next port stops are *too* short in this case less than 3 hours
is_port_too_short AS ( 
  SELECT
  trip_id,
  ssvid,
  trip_start,
  trip_end,
    trip_start_confidence,
  trip_end_confidence,
  trip_start_anchorage_id ,
  trip_end_anchorage_id,
  current_port_too_short,
  port_stop_duration_hr,
  LAG(current_port_too_short, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_port_too_short,
  LEAD(current_port_too_short, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_port_too_short
  FROM (
  SELECT
  trip_id,
  ssvid,
  trip_start,
  trip_end ,
  trip_start_anchorage_id ,
  trip_end_anchorage_id,
  port_stop_duration_hr,
    trip_start_confidence,
  trip_end_confidence,
  ------
  --This clause is to restrict port visits that are less than 3 hours in an attempt to remove port visits that may be associated with 
  ------offloading of fish, and thus would take longer
  ------
  IF((port_stop_duration_hr < 3 AND port_stop_duration_hr IS NOT NULL), TRUE, FALSE) current_port_too_short
  FROM trip_ids)),
-----------------------------------------------------------------
--Label voyages as ones that are "good", ones where we want to use the "start" time, 
--ones where we want to use the "end" time, and ones that we want to remove "remove"
label_trips AS (
  SELECT
  ssvid,
  trip_id,
  trip_start_anchorage_id,
  trip_end_anchorage_id,
  trip_start,
  trip_end,
    trip_start_confidence,
  trip_end_confidence,
  LEAD(trip_end_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_end_anchorage_id,
  LAG(trip_start_anchorage_id, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS prev_start_anchorage_id,
  current_port_too_short,
  prev_port_too_short,
  trip_type,
  port_stop_duration_hr,
  next_voyage_end,
  next_port_stop_duration_hr
  FROM (
  SELECT
  *,
  LEAD(trip_end, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_end,
  LEAD(port_stop_duration_hr, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_port_stop_duration_hr
  FROM (
  SELECT
  ssvid,
  trip_id,
  trip_start_anchorage_id,
  trip_end_anchorage_id,
  trip_start,
  trip_end,
    trip_start_confidence,
  trip_end_confidence,
  current_port_too_short,
  prev_port_too_short,
  CASE
  WHEN current_port_too_short IS FALSE AND (prev_port_too_short IS NULL OR prev_port_too_short IS FALSE) THEN "good_trip"
  WHEN current_port_too_short IS TRUE AND prev_port_too_short IS FALSE THEN "start"
  WHEN current_port_too_short IS TRUE AND prev_port_too_short IS TRUE THEN "remove"
  WHEN current_port_too_short IS FALSE AND prev_port_too_short IS TRUE THEN "end"
  ELSE NULL
  END AS trip_type,
  port_stop_duration_hr
  FROM
  is_port_too_short)
  WHERE trip_type != "remove")),
----------------------------------------------------------------
--update with appropriate trip start/trip ends anchorage_ids, voyage duration, and port duration
updated_voyages AS ( 
  SELECT
  trip_id,
  ssvid,
    trip_start_confidence,
  trip_end_confidence,
  CASE
  WHEN trip_type = "good_trip" THEN trip_start
  WHEN trip_type = "start" THEN trip_start
  ELSE NULL
  END AS trip_start,
  CASE
  WHEN trip_type = "good_trip" THEN trip_start_anchorage_id
  WHEN trip_type = "start" THEN trip_start_anchorage_id
  ELSE NULL
  END AS trip_start_anchorage_id,
  CASE
  WHEN trip_type = "good_trip" THEN trip_end
  WHEN trip_type = "start" THEN next_voyage_end
  ELSE NULL
  END AS trip_end,
  CASE
  WHEN trip_type = "good_trip" THEN trip_end_anchorage_id
  WHEN trip_type = "start" THEN next_end_anchorage_id
  ELSE NULL
  END AS trip_end_anchorage_id,
  CASE
  WHEN trip_type = "good_trip" THEN port_stop_duration_hr
  WHEN trip_type = "start" THEN next_port_stop_duration_hr
  ELSE NULL
  END AS port_stop_duration_hr
  FROM
  label_trips
  WHERE trip_type != "end"),
---------------------------------------------
--Add start anchorage labels and long/lat to trips
trip_start_label AS (
  SELECT
  trip_id,
  ssvid,
  trip_start,
  trip_end,
  trip_start_confidence,
  trip_end_confidence,
  trip_start_anchorage_id,
  b.lat start_anchorage_lat,
  b.lon start_anchorage_lon,
  b.label start_anchorage_label,
  b.iso3 start_anchorage_iso3,
  trip_end_anchorage_id,
  TIMESTAMP_DIFF(trip_end, trip_start, SECOND)/3600 AS trip_duration_hr,
  port_stop_duration_hr
  FROM (
  SELECT
  *
  FROM
  updated_voyages) a
  LEFT JOIN (
  SELECT
  *
  FROM
  `gfw_research.named_anchorages`
  ) b
  ON a.trip_start_anchorage_id = b.s2id
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14),
-----------------------------------
--Add end anchorage labels and long/lat to trips
------------------------------------
trip_end_label as(
  SELECT
  trip_id,
  ssvid,
  trip_start,
  trip_end,
    trip_start_confidence,
  trip_end_confidence,
  trip_start_anchorage_id,
  start_anchorage_lat,
  start_anchorage_lon,
  start_anchorage_label,
  start_anchorage_iso3,
  trip_end_anchorage_id,
  b.lat end_anchorage_lat,
  b.lon end_anchorage_lon,
  b.label end_anchorage_label,
  b.iso3 end_anchorage_iso3,
  trip_duration_hr,
  port_stop_duration_hr
  FROM (
  SELECT
  *
  FROM
  trip_start_label) a
  LEFT JOIN (
  SELECT
  *
  FROM
  `gfw_research.named_anchorages`
  ) b
  ON a.trip_end_anchorage_id = b.s2id
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
-------------
SELECT
*
FROM
trip_end_label
WHERE 
trip_start <='2018-12-31'
AND
  trip_end >='2018-01-01'
AND 
ssvid='352894000'