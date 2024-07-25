 WITH

-- AIS data from research messages 
activity AS (
  SELECT 
    *
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
  -- filter to date period of interest 
  _partitiontime BETWEEN '2017-10-24' AND '2017-11-06'
  -- filter to voi 
  AND ssvid IN ('352894000')
    ),

-- find good segs from activity of interest 
good_segs AS (
  SELECT 
    seg_id 
  FROM 
    `pipe_production_v20201001.research_segs`
  WHERE 
    seg_id IN (SELECT seg_id FROM activity)
    AND  good_seg = TRUE
)


SELECT 
  * 
FROM 
  activity 
WHERE 
  seg_id IN (SELECT DISTINCT seg_id FROM good_segs)
  