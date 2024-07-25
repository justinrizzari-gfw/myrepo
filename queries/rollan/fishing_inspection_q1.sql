# Vessel tracks

WITH

vessel_info AS (
  SELECT
    ssvid,
    best.best_flag as best_flag,
    best.best_vessel_class as best_vessel_class,
    ais_identity.n_shipname_mostcommon.value as n_shipname,
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_v20230501`
  WHERE
    ssvid = '367650000'
),


good_segs AS (
SELECT
  seg_id
FROM `pipe_production_v20201001.research_segs`
WHERE
  good_seg 
  AND positions > 10
  AND NOT overlapping_and_short
),

positions AS (
SELECT
  ssvid,
  seg_id,
  timestamp,
  lon,
  lat,
  hours,
  nnet_score,
  night_loitering
FROM  `pipe_production_v20201001.research_messages`
WHERE ssvid = {ssvid}
AND EXTRACT(DATE FROM _partitiontime) BETWEEN {start_date} AND {end_date}
AND seg_id IN (
  SELECT seg_id
  FROM good_segs
 )
),

positions_with_vessel_info AS (
SELECT *
FROM positions
LEFT JOIN vessel_info
USING (ssvid)
),

fishing_positions AS (
  SELECT
    *,
    CASE
      WHEN best_vessel_class = 'squid_jigger' AND night_loitering = 1 THEN hours
      WHEN best_vessel_class != 'squid_jigger' AND nnet_score > 0.5 THEN hours
    ELSE NULL END AS fishing_hours
  FROM positions_with_vessel_info
)

SELECT *
FROM fishing_positions
ORDER BY timestamp
  