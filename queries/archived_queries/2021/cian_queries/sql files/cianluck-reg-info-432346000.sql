###########################################################
#QUERY: Vessel registry info for MMSI 432346000 between
# 1st Jan 2017 and 10 Feb 2017
#
###########################################################


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
WHERE identity.ssvid = '432346000' 
    AND a.last_timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-02-10 23:59:59' 
    OR identity.ssvid = '432346000' 
    AND a.first_timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-02-10 23:59:59'

# not currently returning any results