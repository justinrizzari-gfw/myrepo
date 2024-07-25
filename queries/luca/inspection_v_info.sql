    SELECT
            ssvid, 
            year,
            ais_identity.shipname_mostcommon.value as shipname, 
            ais_identity.n_imo_mostcommon.value as imo,
            ais_identity.n_callsign_mostcommon.value as callsign,
            best.best_flag,
            best.best_vessel_class
           
        FROM 
            `world-fishing-827.gfw_research.vi_ssvid_byyear_v20230301` where ssvid = '367650000' and year = 2017
