--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 7
--
-- Author: Cian Luck
-- Date: 16 June 2023
--------------------------------------------------------------------------------

-- Was the vessel ‘authorized’ by any RFMO during 2018? If so, which ones and 
-- what are the registry periods?

WITH

  ------------------------------------------------------------------------------
  -- extract activity and registry info pertaining to ssvid '353154000' in 2018
  --
  -- select all identity records where the vessel activity overlapped with 
  -- available registry information
  --
  -- AND
  --
  -- where the period of authorisation overlapped with 2018
  ------------------------------------------------------------------------------
  registry_info AS (
    SELECT
      identity.ssvid AS auth_ssvid,
      shipname AS auth_shipname,
      identity.imo AS auth_imo,
      flag AS auth_flag,
      authorized_from AS registry_authorized_from,
      authorized_to AS registry_authorized_to,
      list_uvi,
      SAFE_CAST( SPLIT(list_uvi, '-')[OFFSET(0)] AS string) AS reg, -- name of registry
      first_timestamp,
      last_timestamp
    FROM
      `vessel_database.all_vessels_v20230501`
      LEFT JOIN UNNEST(registry)
      LEFT JOIN UNNEST(activity)
    WHERE
      identity.ssvid = '353154000'
      --AND authorized_to>first_timestamp
      --AND authorized_from<last_timestamp
      AND 2018 BETWEEN EXTRACT(YEAR FROM authorized_from) AND EXTRACT(YEAR FROM authorized_to) 
    )

--------------------------------------------------------------------------------
-- return these records
--------------------------------------------------------------------------------
SELECT DISTINCT *
FROM registry_info