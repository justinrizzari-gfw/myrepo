-- 19 Sep 2023
-- MS 
-- fishing inspection q4 
-- Provide the other vessel identity information and registry records during this time.

-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('367650000');
CREATE TEMP FUNCTION year() AS (2017);

WITH 

auth AS (
  SELECT
  DISTINCT
    identity.ssvid, 
    identity.n_shipname, 
    list_uvi, 
    geartype_original,
    authorized_from, 
    authorized_to, 
  FROM
    `world-fishing-827.vessel_database.all_vessels_v20230701`
  LEFT JOIN UNNEST(registry)
  LEFT JOIN UNNEST(activity)
  WHERE
    identity.ssvid = mmsi() 
    AND authorized_from > first_timestamp
    AND authorized_to < last_timestamp
    AND EXTRACT(YEAR FROM authorized_from) = year()
  ) 
  
SELECT * FROM auth