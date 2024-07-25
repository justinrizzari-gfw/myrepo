with
  good_segments AS (
  SELECT
    seg_id
  FROM
    `world-fishing-827.gfw_research.pipe_v20201001_segs`
  WHERE good_seg
  AND NOT overlapping_and_short
    ),
  
  fishing_positions AS (
  SELECT
    ssvid,
    timestamp,
    lat,
    lon,
    hours,
    nnet_score,
    IF(nnet_score > 0.5, hours, 0) as fishing_hours
  FROM
    `world-fishing-827.gfw_research.pipe_v20201001`

  WHERE _partitiontime BETWEEN '2017-03-01' AND '2017-03-05'
  AND ssvid = '367650000'

  AND seg_id IN (
    SELECT
      seg_id
    FROM
      good_segments)),


fishing_effort AS (SELECT
ssvid,
sum(fishing_hours) as fishing_hours_pipe
FROM fishing_positions
GROUP BY ssvid),

fishing_events AS (SELECT
ssvid,
sum(hours) as fishing_event_hours
FROM(
SELECT
*,
TIMESTAMP_DIFF(event_end,event_start,SECOND)/3600 as hours,
    JSON_EXTRACT_SCALAR(event_vessels,
 "$[0].ssvid") as ssvid
  FROM
    `pipe_production_v20201001.published_events_fishing`
  WHERE
    event_start >= TIMESTAMP("2017-03-01")
    AND  event_start <= TIMESTAMP("2017-03-05")
    AND
    JSON_EXTRACT_SCALAR(event_vessels,
 "$[0].ssvid") = '367650000'
)
GROUP BY
ssvid)


SELECT 
* FROM fishing_effort AS a 
JOIN fishing_events AS b
ON a.ssvid = b.ssvid 
