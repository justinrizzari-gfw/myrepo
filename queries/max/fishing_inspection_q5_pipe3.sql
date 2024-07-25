-- 18 Sep 2023
-- MS 
-- fishing inspection q5 using pipe 3  
-- Let’s take the scale up for a second. Plot a raster of all fishing by Chinese flagged squid jiggers vessels in NPFC in 2018. What are some caveats to take into consideration with this data?-- set variables for table query

CREATE TEMP FUNCTION flag() AS ('CHN');
CREATE TEMP FUNCTION year() AS ('2018');
CREATE TEMP FUNCTION gear() AS ('squid_jigger');
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP('2018-01-01'));
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP('2018-12-31'));


WITH

  ----------------------------------------------------------
  -- Identify chinese flagged squid jigging vessels in 2018
  ----------------------------------------------------------
  fishing_vessels AS(
    SELECT
      identity.ssvid,
      identity.flag, 
      identity.imo, 
      identity.n_callsign AS callsign, 
      identity.n_shipname AS vessel_name,
      geartype 
    FROM
      `world-fishing-827.pipe_ais_v3_alpha_published.identity_all_vessels_v20231001`
    LEFT JOIN UNNEST(activity) as activity
    LEFT JOIN UNNEST(feature.geartype) as geartype
    WHERE
      identity.flag = flag()
      AND activity.first_timestamp <= end_date()
      AND activity.last_timestamp >= start_date()
      AND is_fishing = TRUE 
      AND geartype = gear()
      ),

  ----------------------------------------------------------
  -- This subquery identifies good segments
  ----------------------------------------------------------
  good_segments AS (
  SELECT
    seg_id
  FROM
    `pipe_ais_v3_alpha_published.segs_activity`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short),

  ----------------------------------------------------------
  -- This subquery fishing query gets all fishing from start_date to end_date
  ----------------------------------------------------------
  fishing AS (
  SELECT
    ssvid,
    /*
    Assign lat/lon bins at desired resolution (here 10th degree)
    FLOOR takes the smallest integer after converting to units of
    0.1 degree - e.g. 37.42 becomes 37.4 10th degree units
    */
    FLOOR(lat * 10) as lat_bin,
    FLOOR(lon * 10) as lon_bin,
    EXTRACT(date FROM timestamp) as date,
    EXTRACT(year FROM timestamp) as year,
    hours,
    nnet_score,
    night_loitering,
    rfmo,
  FROM
    `pipe_ais_v3_alpha_published.messages`
       --, aoi
    LEFT JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo")) AS rfmo
  -- Restrict query to specific time range
  WHERE
  ssvid IN (SELECT DISTINCT ssvid FROM fishing_vessels)
  -- filter to only include data from 2018
  AND timestamp BETWEEN  start_date() AND  end_date()
  -- positions in npfc rfmo
  AND rfmo = 'NPFC'
  -- Use good_segments subquery to only include positions from good segments
  AND seg_id IN (
    SELECT
      seg_id
    FROM
      good_segments)
    ),

  ----------------------------------------------------------
  -- Filter fishing to just the list of active fishing vessels in that year
  ----------------------------------------------------------
  fishing_filtered AS (
  SELECT *
  FROM fishing
  LEFT JOIN fishing_vessels
  -- Only keep positions for fishing vessels active that year
  USING(ssvid)
  ),

  ----------------------------------------------------------
  -- Create fishing_hours attribute. Use night_loitering instead of nnet_score as indicator of fishing for squid jiggers
  ----------------------------------------------------------
  fishing_hours_filtered AS (
  SELECT *,
    CASE
      WHEN geartype = 'squid_jigger' and night_loitering = 1 THEN hours
      WHEN geartype != 'squid_jigger' and nnet_score > 0.5 THEN hours
      ELSE NULL
    END
    AS fishing_hours
  FROM fishing_filtered
  ),


----------------------------------------------------------
-- Return fishing data
----------------------------------------------------------
SELECT *
FROM fishing_hours_filtered
  WHERE fishing_hours > 0 