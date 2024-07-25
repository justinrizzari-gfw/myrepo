-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 1
--
-- Author: Floriane
-- Date: 20 September 2023
--------------------------------------------------------------------------------

WITH


  vessel_info AS (
    SELECT
      ssvid,
      best.best_flag AS best_flag,
      best.best_vessel_class AS best_vessel_class,
      ais_identity.n_shipname_mostcommon.value AS vessel_name
    FROM
      `world-fishing-827.gfw_research.vi_ssvid_v20230801`
    WHERE
      ssvid = '367650000'
  ),
  
  
  track AS (
  SELECT
    ssvid,
    timestamp,
    lon,
    lat,
    hours,
    nnet_score
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
    ssvid = '367650000'
  AND EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-03-01' AND '2017-03-05'
  ),

  
  track_filtered AS (
  SELECT *
  FROM track
  JOIN vessel_info
  USING (ssvid)
  )
  

SELECT *
FROM track_filtered
  

