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
 )


--Identify restricted loitering events by carriers
  --time range of carriers should overlap with the time of encounters to ensure they are actively transmitting during
  --as carriers during the time of encounters
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
AND carrier_flag = 'PAN'
