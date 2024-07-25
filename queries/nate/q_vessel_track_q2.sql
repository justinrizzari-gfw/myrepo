--------------------------------------------------------------------------------
-- Analysis training - Vessel Tracks
-- Question 2
--
-- Author: Cian Luck
-- Date: 15 June 2023
--------------------------------------------------------------------------------

-- Based on what you are seeing, investigate the connection between ssvid, 
-- vessel_id, and seg_id using `pipe_prodcution_v20201001.segment_info` and
-- `pipe_production_v20201001.research_segs`


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
  -- try running the query with and without this filter
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
    -- append vessel_id using segment_info table
  LEFT JOIN (
    SELECT 
      seg_id,
      vessel_id
    FROM
      `pipe_ais_v3_published.segment_info`
  ) 
  USING (seg_id)
  )
  
--------------------------------------------------------------------------------
-- return the distinct values of ssvid, vessel_id, and vessel_name
-- 
-- try running this with and without the good_segments filter
-------------------------------------------------------------------------------- 
SELECT DISTINCT ssvid, vessel_id, vessel_name
FROM track_filtered