--------------------------------------------------------------------------------
-- Analysis training - Fishing inspection
-- Question 4
--
-- Author: Cian Luck
-- Date: 19 June 2023
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
      `gfw_research.vi_ssvid_byyear_v20230501`
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
      identity.ssvid as auth_ssvid,
      shipname as auth_shipname,
      identity.imo as auth_imo,
      flag as auth_flag,
      authorized_from as registry_authorized_from,
      authorized_to as registry_authorized_to,
      SAFE_CAST( SPLIT(list_uvi, '-')[OFFSET(0)] AS string) AS reg, -- name of registry
      first_timestamp,
      last_timestamp
    FROM
      `vessel_database.all_vessels_v20230501`
      LEFT JOIN UNNEST(registry)
      LEFT JOIN UNNEST(activity)
      LEFT JOIN UNNEST(feature.geartype) as feature_gear
    WHERE
      identity.ssvid = '367650000'
      AND authorized_to>first_timestamp
      AND authorized_from<last_timestamp
      AND first_timestamp <= '2017-03-05'
      AND last_timestamp >= '2017-03-01'
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