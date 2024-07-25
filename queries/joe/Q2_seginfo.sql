
-- Q2 query
WITH
segs AS(
  SELECT *
  FROM `gfw_research.pipe_v20201001_segs`
  WHERE ssvid = '352894000' 
  AND first_timestamp >= TIMESTAMP("2017-10-24")
  AND last_timestamp <= TIMESTAMP("2017-11-06")),

seg_info AS(
  SELECT *
  FROM segs
  LEFT OUTER JOIN(
    SELECT *
    FROM `pipe_production_v20201001.segment_info`
    WHERE ssvid = '352894000' 
    AND first_timestamp >= TIMESTAMP("2017-10-24")
    AND last_timestamp <= TIMESTAMP("2017-11-06")
  )
  USING (seg_id)
  
)

SELECT * FROM seg_info
