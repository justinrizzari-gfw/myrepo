-- 25 Aug 2023
-- MS 
-- vessel_info q7 
-- Was the vessel ‘authorized’ by any RFMO during 2018? If so, which ones and what are the registry periods?

-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('353154000');
CREATE TEMP FUNCTION year() AS (2018);

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