WITH
  track AS (
  SELECT
    ssvid,
    seg_id,
    timestamp,
    lon,
    lat,
    hours
  FROM
    `pipe_production_v20201001.research_messages`
  where
    ssvid = '352894000'
  and extract(date from _partitiontime) between '2017-10-24' and '2017-11-06'
  ),

  seg_info  AS (
  SELECT *
  FROM track
  LEFT JOIN (
    SELECT 
      seg_id,
      vessel_id
    FROM
      `pipe_production_v20201001.segment_info`
  ) 
  USING (seg_id)
  )
  
select distinct ssvid, vessel_id,seg_id
from seg_info