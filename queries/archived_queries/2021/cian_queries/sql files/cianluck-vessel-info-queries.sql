############################################################
# QUERY - Get vessel info for mmsi 353154000 during 2018
#
# Part of data analysis training questions
#
# Author: Cian Luck
# Date: 28/07/2021
############################################################

############################################################
# Note that this is a collection of individual queries rather
# than one cohesive query
############################################################


############################################################
# Q1. What is the name, callsign, flag and imo of vessel with
# mmsi 353154000 
# Pull from gfw_research.vi_ssvid_byyear_v20210706
SELECT
    ssvid, 
    registry_info.best_known_shipname,
    registry_info.best_known_callsign,
    registry_info.best_known_flag,
    registry_info.best_known_imo,
    best.best_vessel_class
    year
FROM 
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
WHERE ssvid = '353154000' 
    AND year = 2018

# FEEDBACK
# 9/10 times use ais_identity mostcommon values for vessel info
# w/in ssvid_byyear_v
# all fields can be wrong but ais_identity fields are least often wrong
# map uses ais_identity fields - make analysis consistent with map


############################################################
# Q2. Is the answer different if you pull from
# vessel_database.all_vessels_v20210601
SELECT
    identity.ssvid,
    identity.n_shipname,
    identity.n_callsign,
    identity.flag,
    identity.imo,
    EXTRACT(year FROM a.last_timestamp) as year,
    -- EXTRACT (year FROM activity.last_timestamp) as year,
FROM 
    `world-fishing-827.vessel_database.all_vessels_v20210601`,
    UNNEST(activity) as a
WHERE identity.ssvid = '353154000' 
    AND matched
    -- AND year = 2018
    -- AND a.last_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59'

# FEEDBACK
# WHERE matched - when AIS matches registry records? double check




############################################################
# Q4. Is the vessel a 'carrier'
SELECT 
    year,
    mmsi,
    vessel_class,
FROM
    `world-fishing-827.vessel_database.carrier_vessels_byyear_v20210601`
WHERE mmsi = '353154000'
    AND year = 2018

# FEEDBACK
# vessel_database.all_vessels_v
# WHERE is.carrier
# AND confidence >= 3 # optional restriction
# AND matched # optional restriction - standard for carrier portal # means ais matched "exactly" to registry


############################################################
# Q5. Is the vessel a fishing vessel?
SELECT
    ssvid, 
    year,
    on_fishing_list_known,
    on_fishing_list_nn,
    on_fishing_list_sr,
    on_fishing_list_best
FROM 
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
WHERE ssvid = '353154000' 
    AND year = 2018

# FEEDBACK
# AND on_fishing_list_best # common/useful filter
# best_vessel_class a combination of inferred_vessel_class_ag and best_known_vessel_class?



############################################################
# Q6. What is the vessel class according to 
# vessel_database.all_vessels_v
SELECT
    identity.ssvid,
    identity.imo,
    feature.geartype,
    is_carrier,
    EXTRACT(year FROM a.last_timestamp) as year,
    -- EXTRACT (year FROM activity.last_timestamp) as year,
FROM 
    `world-fishing-827.vessel_database.all_vessels_v20210601`,
    UNNEST(activity) as a
WHERE identity.ssvid = '353154000' 
    -- AND year = 2018
    AND a.last_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59'

# FEEDBACK
#



############################################################
# Q7. Was the vessel authorized by any RFMO during 2018?
# according to gfw_research.vi_ssvid_byyear_v20210706
SELECT
    ssvid, 
    year,
    registry_info.registries_listed
FROM 
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
WHERE ssvid = '353154000' 
    AND year = 2018

# according to vessel_database.all_vessels_v20210601
SELECT
    identity.ssvid,
    identity.imo,
    r.list_uvi,
    r.authorized_from,
    r.authorized_to,
    EXTRACT(year FROM a.last_timestamp) as year,
FROM 
    `world-fishing-827.vessel_database.all_vessels_v20210601`,
    UNNEST(activity) as a,
    UNNEST(registry) as r
WHERE identity.ssvid = '353154000' 
    AND a.last_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59'

# FEEDBACK
# Very important to check that activity period and authorised periods overlap!!
# See Hannah's query
# Also see Willa's github queries for transhipment reports




