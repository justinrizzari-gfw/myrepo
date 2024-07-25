---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2020-01-01'));
---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2020-12-31'));
--SET loitering minimum duration, currently set as 1 hour here which is the default in carrier vessel portal

--HANNAH NOTE: this is correct to match the CVP, however it is important to note that in things like the transshipment reports, -----this duration is increased to 4 hours in order to increase the likelyhood the loitering is related to 'transshipment'. This is
--largely based on work Nate conducted in Miller et al. 2018 along with some work done with Trygg Matt Tracking
CREATE TEMP FUNCTION duration() AS (CAST(1 AS INT64));
--SET loitering average distance from shore (nm),currently set at 20 nm here
CREATE TEMP FUNCTION dist_from_shore() AS (CAST(20 AS INT64));
#####
---create curated carrier list
WITH carrier_vessels AS (
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
--HANNAH note: here it is important to use the most recent version of vessel database (that has been verified by Jaeyoon and other ---that it is okay for analysts to use)
`world-fishing-827.vessel_database.all_vessels_v20201201`
LEFT JOIN UNNEST(registry)
LEFT JOIN UNNEST(activity)
LEFT JOIN UNNEST(feature.geartype) as feature_gear
WHERE is_carrier 
AND
confidence >= 3
AND
identity.ssvid NOT IN ('111111111','0','888888888','416202700')
AND
DATE(first_timestamp) <= maximum()
AND DATE(last_timestamp) >= minimum()
GROUP BY 1,2,3,4,5,6,7,8),
####
 --Search for only carrier vessels in loitering table, specifying lat,lon, time, and minimum duration of event
 --Note the ST_CENTROID function calculated the the lat/lon between the start and end lat/lon values
 --Note that I specify distance from shore, minimum loitering duration, and ensure the segments are considered 'good' aka less noisy
 loitering as(
 SELECT
 *
 FROM(
 SELECT
vessel_id,
loitering_start_timestamp,
  loitering_end_timestamp,
  loitering_hours,
  tot_distance_nm,
  avg_speed_knots,
  avg_distance_from_shore_nm,
  start_lon,
  start_lat,
  end_lon,
  end_lat,
   ST_X(centroid) as mean_lon,
   ST_Y(centroid) as mean_lat
FROM(
SELECT
  ssvid as vessel_id,
  loitering_start_timestamp,
  loitering_end_timestamp,
  loitering_hours,
  tot_distance_nm,
  avg_speed_knots,
  avg_distance_from_shore_nm,
  start_lon,
  start_lat,
  end_lon,
  end_lat,
  ST_CENTROID( ST_UNION(ST_GEOGPOINT(start_lon,
          start_lat),
        ST_GEOGPOINT(end_lon,
          end_lat)) ) centroid
FROM
  `gfw_research.loitering_events_v20200205` 
WHERE
ssvid IN (SELECT
carrier_ssvid
FROM
carrier_vessels) AND
DATE(loitering_start_timestamp) >= minimum() AND
DATE(loitering_end_timestamp) <= maximum() AND
avg_distance_from_shore_nm > dist_from_shore() AND
loitering_hours>=duration()
AND
--removes loitering events associated with 'noisey' segment IDs
seg_id IN (
  SELECT
    seg_id
  FROM
    `gfw_research.pipe_v20190502_segs`
  WHERE
    good_seg
    AND
NOT overlapping_and_short))
    GROUP BY
    vessel_id,
  loitering_start_timestamp,
  loitering_end_timestamp,
  loitering_hours,
  tot_distance_nm,
  avg_speed_knots,
  avg_distance_from_shore_nm,
  start_lon,
  start_lat,
  end_lon,
  end_lat,
  mean_lon,
  mean_lat
 )
 ),


--Identify restricted loitering events by carriers
  --time range of carriers should overlap with the time of encounters to ensure they are actively transmitting during
  --as carriers during the time of encounters
loits_f as(
SELECT
vessel_id,
start_lat,
start_lon,
end_lat,
end_lon,
  mean_lon,
  mean_lat,
loitering_start_timestamp,
  loitering_end_timestamp,
  loitering_hours,
  tot_distance_nm,
  avg_speed_knots,
  avg_distance_from_shore_nm
FROM(
SELECT *
FROM loitering
)a
JOIN(
SELECT
carrier_ssvid,
carrier_first_timestamp,
carrier_last_timestamp,
carrier_flag
FROM
carrier_vessels)b
ON
SAFE_CAST(a.vessel_id as STRING)=SAFE_CAST(b.carrier_ssvid as STRING)
AND
a.loitering_start_timestamp BETWEEN b.carrier_first_timestamp AND b.carrier_last_timestamp
AND
a.loitering_end_timestamp BETWEEN b.carrier_first_timestamp and b.carrier_last_timestamp
AND carrier_flag = 'PAN'),


----HANNAH NOTE: up until this point everything was the same as query for Q5

 #############Voyages linked to encounters

#standardsql
--#
--##
--# Get voyage trip ids and
--# identify the duration of port
--# visit following each voyage
--##
--#

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
`pipe_production_v20190502.voyages`)))),


--#
--##
--# identify if the current, previous, or
--# next port stops are *too* short
--# in this case less than 6 hours
--##
--#

is_port_too_short AS (SELECT
trip_id,
ssvid,
trip_start,
trip_end,
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
------
--HANNAH NOTE: this clause is to restrict port visits that are less than 3 hours in an attempt to remove port visits that may either be false to AIS spoofing OR unrelated to port visits that may be associated with offloading of fish, and thus would take longer
------
IF((port_stop_duration_hr < 3 AND port_stop_duration_hr IS NOT NULL), TRUE, FALSE) current_port_too_short
FROM trip_ids)),

--#
--###
--# Label voyages as ones that are
--# "good", ones where we want to use
--# the "start" time, ones where we want
--# to use the "end" time, and ones that
--# we want to remove "remove"
--###
--#
--#
label_trips AS (
SELECT
ssvid,
trip_id,
trip_start_anchorage_id,
trip_end_anchorage_id,
trip_start,
trip_end,
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


#
#
###
# update with appropriate trip start/trip ends
# anchorage_ids, voyage duration, and port duration
###
#
#
updated_voyages AS (SELECT
trip_id,
ssvid,
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

--#
--##
--# Add start anchorage labels
--# and long/lat to trips
--##
--#
--#
trip_start_label AS (
SELECT
trip_id,
ssvid,
trip_start,
trip_end,
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
group by 1,2,3,4,5,6,7,8,9,10,11,12),
--#
--#
--##
--# Add end anchorage labels
--# and long/lat to trips
--##
--#
--#
trip_end_label AS (
SELECT
trip_id,
ssvid,
trip_start,
trip_end,
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
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16),

all_loit_data as(
SELECT
vessel_id,
start_lat,
start_lon,
end_lat,
end_lon,
  mean_lon,
  mean_lat,
loitering_start_timestamp,
  loitering_end_timestamp,
  loitering_hours,
  tot_distance_nm,
  avg_speed_knots,
  avg_distance_from_shore_nm,
trip_id as Carrier_trip_id,
trip_start as Carrier_trip_start,
trip_end as Carrier_trip_end,
trip_start_anchorage_id as carrier_trip_start_anchorage_id,
start_anchorage_lat as carrier_start_anchorage_lat,
start_anchorage_lon as carrier_start_anchorage_lon,
start_anchorage_label as carrier_start_anchorage_label,
start_anchorage_iso3 as carrier_start_anchorage_iso3,
trip_end_anchorage_id as carrier_trip_end_anchorage_id,
end_anchorage_lat as carrier_end_anchorage_lat,
end_anchorage_lon as carrier_end_anchorage_lon,
end_anchorage_label as carrier_end_anchorage_label,
end_anchorage_iso3 as carrier_end_anchorage_iso3
FROM(
SELECT
*
FROM
loits_f)a
LEFT JOIN
(
SELECT
* FROM
trip_end_label) b
ON SAFE_CAST(a.vessel_id as int64)= SAFE_CAST(b.ssvid as int64)
WHERE (a.loitering_start_timestamp BETWEEN b.trip_start AND
b.trip_end
AND a.loitering_end_timestamp BETWEEN b.trip_start AND
b.trip_end )

OR
(a.loitering_start_timestamp >= b.trip_start AND
b.trip_end = timestamp("9999-09-09")
AND a.loitering_end_timestamp >= b.trip_start AND
b.trip_end = timestamp("9999-09-09"))

OR
(a.loitering_start_timestamp <= b.trip_end AND
b.trip_start = timestamp("0001-02-03")
AND a.loitering_end_timestamp <= b.trip_end AND
b.trip_start = timestamp("0001-02-03")
)),

clean_loits as (
SELECT
*
FROM
all_loit_data)

SELECT
carrier_end_anchorage_label
FROM
clean_loits
GROUP BY
carrier_end_anchorage_label
ORDER BY 
carrier_end_anchorage_label

