select
ssvid,
lon,
lat,
timestamp,
extract(date from _partitiontime) as date,
speed_knots,
-1 * elevation_m AS depth_m,
distance_from_shore_m/1000 AS distance_from_shore_km,
nnet_score,
hours,
ST_MAKELINE(
    ST_GEOGPOINT(lon,lat),
    ST_GEOGPOINT(LAG (lon,1) OVER (PARTITION BY ssvid ORDER BY timestamp),
    LAG (lat,1) OVER (PARTITION BY ssvid ORDER BY timestamp))) as line_segment -- make line segment 
from
  `pipe_production_v20201001.research_messages`
  WHERE seg_id IN (
SELECT seg_id
FROM  `pipe_production_v20201001.research_segs`
WHERE
overlapping_and_short IS FALSE AND
good_seg IS TRUE) AND
ssvid = '367650000' 
and extract(date from _partitiontime) between '2017-03-01' and '2017-03-05'