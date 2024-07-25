--------------------------------------------------------------------------------
-- Analysis training - Vessel info
-- Question 7
--
-- Author: Nate Miller
-- Date: 22 May 2024
--------------------------------------------------------------------------------
--
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
      ssvid AS auth_ssvid,
      n_shipname AS auth_shipname,
      n_callsign AS auth_callsign,
      imo AS auth_imo,
      flag AS auth_flag,
      authorized_from AS registry_authorized_from,
      authorized_to AS registry_authorized_to,
      source_code
    FROM
    `pipe_ais_v3_published.identity_authorization_v20240401`

    WHERE
      ssvid = '353154000'
      AND 2018 BETWEEN EXTRACT(YEAR FROM authorized_from) AND EXTRACT(YEAR FROM authorized_to) 
    )

--------------------------------------------------------------------------------
-- return these records
--------------------------------------------------------------------------------
SELECT DISTINCT *
FROM registry_info