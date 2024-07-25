 WITH

-- AIS data from research messages 
activity AS (
  SELECT 
    *
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
  -- filter to date period of interest - narrow for out of sequence positions
  _partitiontime BETWEEN '2017-10-26' AND '2017-11-01'
  -- filter to voi 
  AND ssvid IN ('352894000')
    ),

-- look at segment information for segments in track of interest
segs AS (
  SELECT 
    *
  FROM
    `world-fishing-827.gfw_research.pipe_v20201001_segs`
  WHERE 
    seg_id IN (SELECT seg_id FROM activity)
  ), 
  
seg_vessel_info AS (
  SELECT 
    seg_id,
    shipname.value AS shipname,
    imo.value AS imo,
    callsign.value AS ircs,
  FROM
    `pipe_production_v20201001.segment_info`
  WHERE 
    seg_id IN (SELECT seg_id FROM activity)
  )
  
SELECT 
  * 
FROM
  segs
LEFT JOIN 
  seg_vessel_info USING (seg_id)