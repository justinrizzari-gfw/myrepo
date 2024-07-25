---------------------------------------------------------------------
-- Query: How many voyages did 'Tuna Queen' have in 2018 with a 
-- port confidence of 4?
--
-- Author: Cian Luck
-- Date: 15 Oct 2021
---------------------------------------------------------------------

WITH 

    -----------------------------------------------------------------
    -- vessel info
    -----------------------------------------------------------------
    vessel_info AS(
        SELECT
            ssvid,
            year,
            ais_identity.shipname_mostcommon.value as shipname,
            best.best_flag,
            best.best_vessel_class
        FROM 
            `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210913`
        WHERE 
            ais_identity.shipname_mostcommon.value = 'TUNA QUEEN'
    ),

    -----------------------------------------------------------------
    -- voyages confidence 4
    -----------------------------------------------------------------
    voyages_c4 AS(
        SELECT 
            ssvid, 
            trip_id,
            trip_start,
            trip_end,
            trip_start_confidence,
            trip_end_confidence,
            EXTRACT(year FROM trip_start) AS year
        FROM 
            `world-fishing-827.pipe_production_v20201001.proto_voyages_c4`
        WHERE 
            DATE(trip_start) BETWEEN "2018-01-01" AND "2018-12-31" 
            OR DATE(trip_end) BETWEEN "2018-01-01" AND "2018-12-31" 
    ),

    -----------------------------------------------------------------
    -- Tuna Queen voyages
    -----------------------------------------------------------------
    vessel_voyages AS(
        SELECT *
        FROM vessel_info
        JOIN voyages_c4
            USING(ssvid, year)
    )

------------------------------------------------------------
-- Return vessel_voyages
------------------------------------------------------------
SELECT *
FROM vessel_voyages
