--------------------------------------------------------------------------------
-- Analysis training - Vessel Tracks
-- Question 3
--
-- Author: Nate Miller
-- Date: 22 May  2024
--------------------------------------------------------------------------------

-- Plot the track for MMSI 352894000 from October 24th 2017 - November 6 2017 
-- again but removing noise.


WITH

  ------------------------------------------------------------------------------
  -- pull vessel identity information from vessel info tables
  ------------------------------------------------------------------------------
  vessel_info AS (
    SELECT
      ssvid,
      best.best_flag AS best_flag,
      best.best_vessel_class AS best_vessel_class,
      ais_identity.n_shipname_mostcommon.value AS vessel_name
    FROM
      -- IMPORTANT: change below to most up to date table
      `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240401`
    WHERE
      ssvid = '352894000'
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
    hours
  FROM
    `world-fishing-827.pipe_ais_v3_published.messages`
  WHERE
    ssvid = '352894000'
  AND EXTRACT(DATE FROM timestamp) BETWEEN '2017-10-24' AND '2017-11-06'
  -- filter to only include good track segments
  AND clean_segs
  ),

  ------------------------------------------------------------------------------
  -- merge with vessel info
  ------------------------------------------------------------------------------
  track_filtered AS (
  SELECT *
  FROM track
  JOIN vessel_info
  USING (ssvid)
  )
  
--------------------------------------------------------------------------------
-- return vessel track
-------------------------------------------------------------------------------- 
SELECT *
FROM track_filtered