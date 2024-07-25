-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

-- SELECT
--     ssvid,
--     seg_id,
--     timestamp,
--     lon,
--     lat,
--     hours,
--     nnet_score
--   FROM
--     `pipe_production_v20201001.research_messages`
--   WHERE
--     ssvid = '352894000'
--   AND EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-10-24' AND '2017-11-06'
--  Exported as `world-fishing-827.scratch_dhiya.q1_352894000_track_2`

WITH

  vessel_info AS (
    SELECT
      ssvid,
      best.best_flag AS b_flag,
      best.best_vessel_class AS b_vessel_class,
      ais_identity.n_shipname_mostcommon.value AS vessel_name,
    FROM
      `world-fishing-827.gfw_research.vi_ssvid_v20230501`
    WHERE
      ssvid = '352894000'
  ),

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

  vessel_track AS (
    SELECT * FROM `world-fishing-827.scratch_dhiya.q1_352894000_track_2`
  ),


merged_filtered AS (
SELECT
a.*,
b.b_flag,
b.b_vessel_class,
b.vessel_name,
c.vessel_id
FROM vessel_track a
LEFT JOIN vessel_info b
USING (ssvid)
LEFT JOIN `pipe_production_v20201001.segment_info` c
USING (seg_id)
WHERE seg_id IN (SELECT * FROM good_segments)  
)

SELECT ssvid, timestamp, lat, lon, hours, nnet_score FROM merged_filtered;
