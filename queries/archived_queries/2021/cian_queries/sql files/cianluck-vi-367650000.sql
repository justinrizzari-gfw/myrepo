###########################################################
#QUERY: Vessel info for MMSI 432346000 between
# 01 Mar 2017 - 05 Mar 2017
#
###########################################################

SELECT
    ssvid,
    year,
    best.best_vessel_class,
    registry_info.registries_listed,
    registry_info.best_known_imo,
    registry_info.best_known_callsign,
    registry_info.best_known_flag
  FROM gfw_research.vi_ssvid_byyear_v20210706
  WHERE ssvid = '367650000'
    AND year = 2017