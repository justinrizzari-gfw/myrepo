------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 5
--
-- Author: all credit to Cian
-- Date: 4 Oct 2023
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
  -- filter to Chinese squid jiggers in 2018
  ----------------------------------------------------------
  fishing_vessels AS (
  SELECT
    DISTINCT ssvid,
    -- year,
    2018 AS year,
    flag,
    geartype,
    -- first_timestamp,
    -- last_timestamp,
    -- timestamp_overlap
  FROM
  -- IMPORTANT: change below to most up to date table - 
  -- from wiki: This table includes the GFW yearly list of active non spoofing/offsetting fishing vessels. 
  -- It is GFW's most restrictive list of fishing vessels and the default list to use in research/analysis.
    -- `gfw_research.fishing_vessels_ssvid_v20230901`
    pipe_ais_v3_alpha_published.identity_core_v20231001 -- currently database / ident core are only ident tables w geartype in pipe3
    -- pipe_ais_v3_alpha_published.identity_all_vessels_v20231001 -- currently database / ident core are only ident tables w geartype in pipe3
  WHERE
    -- best_flag = 'CHN'
    flag = 'CHN'
    AND is_fishing
    -- AND best_vessel_class = 'squid_jigger'
    AND geartype = 'squid_jigger'
    AND EXTRACT(YEAR FROM first_timestamp) <= 2018
    AND EXTRACT(YEAR FROM last_timestamp) >= 2018
  ),

  ----------------------------------------------------------
  -- This fishing subquery gets all fishing from start_date to end_date
  -- It queries the pipe_vYYYYMMDD_fishing table, which includes only likely
  -- fishing vessels.
  ----------------------------------------------------------
  fishing AS (
  SELECT 
    ssvid,
  
    -- Assign lat/lon bins at desired resolution (here 10th degree)
    -- FLOOR takes the smallest integer after converting to units of
    -- 0.1 degree - e.g. 37.42 becomes 374 10th degree units
  
    FLOOR(lat * 10) as lat_bin,
    FLOOR(lon * 10) as lon_bin,
    -- EXTRACT(date FROM _partitiontime) as date,
    -- EXTRACT(year FROM _partitiontime) as year,
    -- timestamp AS date,
    EXTRACT(year FROM timestamp) as year,
    EXTRACT(date FROM timestamp) as date,
    hours,
    nnet_score,
    night_loitering,
    rfmo
    -- regions
    -- JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo") AS rfmo
    -- JSON_EXTRACT(regions, "$.rfmo") AS rfmo
  FROM
    -- `pipe_production_v20201001.research_messages`
    `pipe_ais_v3_alpha_published.messages`,
  -- left join regions.rfmo to access rfmo information
  -- LEFT JOIN UNNEST(regions.rfmo) AS rfmo
  -- LEFT JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo")) AS rfmo -- i think left join is not needed, just unnest below gives same result
  UNNEST(JSON_EXTRACT_STRING_ARRAY(regions, "$.rfmo")) AS rfmo
 
  WHERE
  -- restrict query to fishing vessels only to keep query cost low
    -- is_fishing_vessel = TRUE -- not available in pipe3
  -- Restrict query to specific time range 
  -- AND _partitiontime BETWEEN '2018-01-01' AND '2018-12-31'
  EXTRACT(DATE from timestamp) BETWEEN '2018-01-01' AND '2018-12-31'
  AND rfmo = 'NPFC'
  -- Use good_segments subquery to only include positions from good segments
  AND seg_id IN (
    SELECT
      seg_id
    FROM
      good_segments)
  AND ssvid IN (
    SELECT 
      DISTINCT ssvid
    FROM fishing_vessels)
  ),
 
  ----------------------------------------------------------
  -- Filter fishing to just the list of active fishing vessels in that year
  ----------------------------------------------------------
  fishing_filtered AS (
  SELECT *
  FROM fishing
  JOIN fishing_vessels
  -- Only keep positions for fishing vessels active that year
  USING(ssvid, year)
  ),

  ----------------------------------------------------------
  -- Create fishing_hours attribute. Use night_loitering instead of nnet_score as indicator of fishing for squid jiggers
  ----------------------------------------------------------
  fishing_hours_filtered AS (
  SELECT *,
    CASE
      -- WHEN best_vessel_class = 'squid_jigger' and night_loitering = 1 THEN hours
      WHEN geartype = 'squid_jigger' and night_loitering = 1 THEN hours
      -- WHEN best_vessel_class != 'squid_jigger' and nnet_score > 0.5 THEN hours
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
    date,
    -- keep ssvid as we're interested in counting number of vessels
    ssvid,

    -- Convert lat/lon bins to units of degrees from 10th of degrees.
    -- 374 now becomes 37.4 instead of the original 37.42
  
    lat_bin / 10 as lat_bin,
    lon_bin / 10 as lon_bin,
    geartype,
    flag,
    -- best_vessel_class,
    -- best_flag,
    rfmo,
    SUM(hours) as hours,
    SUM(fishing_hours) as fishing_hours,
  FROM fishing_hours_filtered
  GROUP BY date, ssvid, lat_bin, lon_bin, geartype, flag, rfmo
  -- GROUP BY date, ssvid, lat_bin, lon_bin, best_vessel_class, best_flag, rfmo
  )

----------------------------------------------------------
-- Return fishing data
----------------------------------------------------------
SELECT *
FROM fishing_binned
