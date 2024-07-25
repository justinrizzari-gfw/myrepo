--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 4
--
-- Author: Nate Miller
-- Date: 24 May 2024
--------------------------------------------------------------------------------

-- Provide the other vessel identity information and registry records during this
-- time.


WITH

  ------------------------------------------------------------------------------
  -- pull vessel info from vi_ssvid_byyear_
  ------------------------------------------------------------------------------
  vessel_info AS (
    SELECT
      ssvid,
      ais_identity.n_shipname_mostcommon.value as shipname,
      ais_identity.n_imo_mostcommon.value as imo,
      ais_identity.n_callsign_mostcommon.value as callsign,
      best.best_flag as flag,
      best.best_vessel_class
    FROM
      `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v20240401`
    WHERE
    ssvid = '367650000'
    AND year = 2017
  ),

  ------------------------------------------------------------------------------
  -- pull available registry info (if any)
  ------------------------------------------------------------------------------
  registry_info AS (
    SELECT
      DISTINCT
      ssvid as auth_ssvid,
      n_shipname as auth_shipname,
      imo as auth_imo,
      flag as auth_flag,
      authorized_from as registry_authorized_from,
      authorized_to as registry_authorized_to,
      source_code AS reg, -- name of registry
    FROM
      `pipe_ais_v3_published.identity_authorization_v20240401`
    WHERE
      ssvid = '367650000'
      AND authorized_to > TIMESTAMP('2017-03-01')
      AND authorized_from < TIMESTAMP('2017-03-05')
    ),
    
  ------------------------------------------------------------------------------
  -- join the vessel info and registry information
  ------------------------------------------------------------------------------
  vessel_registry_info AS (
      SELECT *
      FROM vessel_info
      LEFT JOIN registry_info
      ON (vessel_info.ssvid = registry_info.auth_ssvid)
    )

--------------------------------------------------------------------------------
-- return vessel_registry_info
--------------------------------------------------------------------------------
SELECT *
FROM vessel_registry_info