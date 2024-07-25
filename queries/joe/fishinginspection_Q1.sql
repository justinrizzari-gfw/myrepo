-- vessel track filtered to good segs and info

WITH

tracks AS (
SELECT
    ssvid,
    seg_id,
    timestamp,
    extract(date from _partitiontime) as date,
    hours,
    lat,
    lon,
    -1 * elevation_m AS depth_m,
    distance_from_shore_m/1000 AS distance_from_shore_km,
    speed_knots,
    nnet_score
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
  EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-03-01' AND '2017-03-05'
  AND ssvid IN ("367650000") 
  AND seg_id IN (
    SELECT 
    seg_id 
    FROM
    `pipe_production_v20201001.research_segs`
    WHERE 
    good_seg IS TRUE AND
    overlapping_and_short IS FALSE)
),

vessel_info AS (
  SELECT 
  ssvid,
  best.best_flag AS best_flag,
  best.best_vessel_class AS best_vessel_class,
  -- ais_identity.n_shipname_mostcommon AS ais_name,
  registry_info.best_known_shipname AS registry_name
  FROM `gfw_research.vi_ssvid_v20230801`
  WHERE
  ssvid IN ("367650000")
  )

SELECT * 
FROM tracks
LEFT JOIN vessel_info
USING (ssvid)




