-- 18 Aug 2023
-- MS 
-- vessel_info q2 vi_ssvid_v
-- Is the above answer different if you pull from the vessel info table (`gfw_research.vi_ssvid_v`) 
-- versus the vessel registry table (`vessel_database.all_vessels_v`)?

-- set variables for table query
CREATE TEMP FUNCTION mmsi() AS ('353154000');
CREATE TEMP FUNCTION start_date() AS (DATE('2018-01-01'));
CREATE TEMP FUNCTION end_date() AS (DATE('2019-01-01'));

WITH 

vessel_info AS(
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
    activity.first_timestamp, 
    activity.last_timestamp
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_v20230701`
  WHERE
    ssvid = mmsi()
    AND DATE(activity.first_timestamp) < start_date() 
    AND DATE(activity.last_timestamp) > end_date()
)

SELECT * FROM vessel_info
