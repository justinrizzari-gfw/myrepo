
    SELECT
            identity.ssvid as ssvid, 
            r.shipname as shipname, 
            r.imo as imo,
            r.callsign as callsign,
            r.flag, 
            first_timestamp,
            last_timestamp,
  r.list_uvi,
  r.authorized_from,
  r.authorized_to,
geartype_original
        FROM 
            `world-fishing-827.vessel_database.all_vessels_v20230701`
            LEFT JOIN UNNEST (registry) as r
            LEFT JOIN UNNEST (activity)
WHERE
 identity.ssvid = '367650000' 
 and DATE(first_timestamp) <='2017-01-05' and
 DATE(last_timestamp) >='2017-01-05'

group by ssvid, shipname,imo, callsign, flag,   first_timestamp, last_timestamp, r.list_uvi,
  r.authorized_from,
  r.authorized_to,
geartype_original