--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 1
--
-- Author: Cian Luck
-- Date: 19 June 2023
--------------------------------------------------------------------------------

-- We may also want to know about fishing. Plot a map of the track and fishing 
-- points by the vessel with the MMSI 367650000 between March 1 2017 and March 
-- 5 2017. Are there any issues with the track?

WITH

  ------------------------------------------------------------------------------
  -- pull vessel identity information from vessel info tables
  ------------------------------------------------------------------------------
  vessel_info AS (
    SELECT
      ssvid,
      best.best_flag AS best_flag,
      best.best_vessel_class AS best_vessel_class,
      ais_identity.n_shipname_mostcommon.value AS vessel_name,
      -- year
    FROM
      -- IMPORTANT: change below to most up to date table
      `world-fishing-827.gfw_research.vi_ssvid_v20230501`
    WHERE
      ssvid = '367650000'
  ),

  -----------------------------------------------------------------------------
  -- Create a list of good track segments
  -----------------------------------------------------------------------------
  good_segments AS (
  SELECT
    seg_id
  FROM
    `pipe_production_v20201001.research_segs`
  WHERE
    good_seg 
    AND positions > 10
    AND NOT overlapping_and_short
    ),

  ------------------------------------------------------------------------------
  -- pull track positions for this vessel
  -- 
  -- limiting the query to one MMSI and a relatively short time frame should 
  -- keep the query cost relatively low
  ------------------------------------------------------------------------------
  track AS (
  SELECT
    ssvid,
    seg_id,
    timestamp,
    lon,
    lat,
    hours,
    nnet_score,
    night_loitering
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
    ssvid = '367650000'
  AND EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-03-01' AND '2017-03-05'
  -- filter to only include good track segments
  AND seg_id IN (
    SELECT seg_id
    FROM good_segments
   )
  ),

  ------------------------------------------------------------------------------
  -- merge with vessel info
  ------------------------------------------------------------------------------
  track_filtered AS (
  SELECT *
  FROM track
  JOIN vessel_info
  USING (ssvid)
  ),
  
  ------------------------------------------------------------------------------
  -- identify fishing points using vessel class and nnet_score/night_loitering
  ------------------------------------------------------------------------------
  fishing_track AS (
    SELECT
      *,
      CASE
        WHEN best_vessel_class = 'squid_jigger' AND night_loitering = 1 THEN hours
        WHEN best_vessel_class != 'squid_jigger' AND nnet_score > 0.5 THEN hours
      ELSE NULL END AS fishing_hours
    FROM track_filtered
  )

--------------------------------------------------------------------------------
-- return the fishing vessel track with fishing hours
-------------------------------------------------------------------------------- 
SELECT *
FROM fishing_track
  