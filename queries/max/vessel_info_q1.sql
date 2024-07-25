-- 18 Aug 2023
-- MS 
-- vessel_info q1 
-- What is the name, callsign, flag state, and imo of the vessel with MMSI 353154000 during 2018?

-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('353154000');
CREATE TEMP FUNCTION year() AS (2018);

WITH 

-- look at registry info and ais_identity info within this table 
identity AS (
  SELECT
    ssvid,
    registry_info.best_known_shipname AS registry_name,
    registry_info.best_known_imo AS registry_imo,
    registry_info.best_known_flag AS registry_flag,
    registry_info.best_known_callsign AS registry_ircs,
    ais_identity.n_shipname_mostcommon.value AS shipname_most_common_AIS,
    ais_identity.n_callsign_mostcommon.value AS callsign_most_common_AIS,
    ais_identity.n_imo_mostcommon.value AS imo_most_common_AIS,
    ais_identity.flag_mmsi,
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_byyear`
  WHERE
    year = year() AND
    ssvid = mmsi()
)  

SELECT * FROM identity
