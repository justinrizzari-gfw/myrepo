------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 5
--
-- Update: Answer question using pipe_ais_v3_alpha_published
--
-- Author: Cian Luck
-- Date: 13 November 2023
------------------------------------------------------------

-- Plot a raster of all fishing by Chinese flagged squid jiggers vessels in NPFC in 2018.

WITH

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
  -- Get the list of active fishing vessels that pass the noise filters
  -- filter to Chinese squid jiggers 
  -- will apply timerange filters later
  ----------------------------------------------------------
  fishing_vessels AS (
  SELECT
      ssvid,
      first_timestamp,
      last_timestamp,
      flag,
      geartype
    FROM `pipe_ais_v3_alpha_published.identity_core_v20231001`
    WHERE 
      is_fishing = TRUE
      AND flag = 'CHN'
      AND geartype = 'squid_jigger'
  ),

  ----------------------------------------------------------
  -- Query the fishing vessel positions from pipe_ais_v3_published.messages
  -- note that we can limit query cost by filtering by timestamp
  -- we should also be able to reduce query costs by filtering by ssvid or flag
  -- but this currently isn't working as expected
  ----------------------------------------------------------
  fishing AS (
  SELECT
    ssvid,
    /*
    Assign lat/lon bins at desired resolution (here 10th degree)
    FLOOR takes the smallest integer after converting to units of
    0.1 degree - e.g. 37.42 becomes 374 10th degree units
    */
    FLOOR(lat * 10) as lat_bin,
    FLOOR(lon * 10) as lon_bin,
    timestamp,
    hours,
    nnet_score,
    night_loitering,
    -- JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo") AS rfmo
    rfmo
  FROM
    `pipe_ais_v3_alpha_published.messages` 
    LEFT JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo")) AS rfmo
  WHERE
  -- Restrict query to specific time range 
  EXTRACT(DATE FROM timestamp) BETWEEN '2018-01-01' AND '2018-12-31'
  -- Use good_segments subquery to only include positions from good segments
  AND seg_id IN (
    SELECT
      seg_id
    FROM
      good_segments)
  -- restrict to just vessels in fishing_vessels
  AND ssvid in (SELECT ssvid FROM fishing_vessels) 
  -- filter to only positions in NPFC
  AND rfmo = "NPFC"
    ),

  ----------------------------------------------------------
  -- Filter fishing to just the list of active fishing vessels in that year
  ----------------------------------------------------------
  fishing_filtered AS (
  SELECT fishing.ssvid, * EXCEPT(ssvid)
  FROM fishing
  JOIN fishing_vessels
  -- Only keep positions for fishing vessels active that year
  ON (
  fishing.ssvid = fishing_vessels.ssvid
  AND fishing.timestamp BETWEEN fishing_vessels.first_timestamp AND fishing_vessels.last_timestamp
   )
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
  -- This subquery sums fishing hours and converts coordinates back to
  -- decimal degrees
  ----------------------------------------------------------
  fishing_binned AS (
  SELECT
    EXTRACT(DATE FROM timestamp) AS date,
    -- keep ssvid as we're interested in counting number of vessels
    ssvid,
    /*
    Convert lat/lon bins to units of degrees from 10th of degrees.
    374 now becomes 37.4 instead of the original 37.42
    */
    lat_bin / 10 as lat_bin,
    lon_bin / 10 as lon_bin,
    geartype,
    flag,
    rfmo,
    SUM(hours) as hours,
    SUM(fishing_hours) as fishing_hours,
  FROM fishing_hours_filtered
  GROUP BY date, ssvid, lat_bin, lon_bin, geartype, flag, rfmo
  )

----------------------------------------------------------
-- Return fishing data
----------------------------------------------------------
SELECT *
FROM fishing_binned