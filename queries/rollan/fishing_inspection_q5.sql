
CREATE TEMP FUNCTION start_date() AS (DATE({start_date}));
CREATE TEMP FUNCTION end_date() AS (DATE({end_date}));
CREATE TEMP FUNCTION flag() AS (CAST({flag} AS STRING));
CREATE TEMP FUNCTION gear() AS (CAST({geartype} AS STRING));

--CREATE TEMP FUNCTION start_date() AS (DATE('2018-01-01'));
--CREATE TEMP FUNCTION end_date() AS (DATE('2018-12-31'));
--CREATE TEMP FUNCTION flag() AS (CAST("CHN" AS STRING));
--CREATE TEMP FUNCTION gear() AS (CAST("squid_jigger" AS STRING));

# Fishing raster

WITH

vessel_info AS (
  SELECT  ssvid,
    best.best_flag as best_flag,
    best.best_vessel_class as best_vessel_class,
    ais_identity.n_shipname_mostcommon.value as n_shipname,
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20230801`
  WHERE best.best_flag = flag()
    AND best.best_vessel_class = gear()
    AND year = EXTRACT(year FROM start_date())
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
WHERE ssvid IN (SELECT ssvid FROM vessel_info)
AND 'NPFC' IN UNNEST(regions.rfmo)
AND EXTRACT(DATE FROM _partitiontime) BETWEEN start_date() AND end_date()
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
),

fishing_binned AS (
  SELECT 
    ssvid,
    n_shipname,
    lat_bin,
    lon_bin,
    SUM(fishing_hours) as fishing_hours
  FROM (
    SELECT 
      *,
      FLOOR(lat * 10) / 10 as lat_bin,
      FLOOR(lon * 10) / 10 as lon_bin,
    FROM fishing_positions
    WHERE fishing_hours IS NOT NULL)
  GROUP BY 1,2,3,4
)

SELECT * 
FROM fishing_binned

  