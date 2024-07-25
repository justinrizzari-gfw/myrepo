---------------------------------------------------------------------
-- Query: Count of port visits per port based on end of voyages in
-- 2018 (confidence >= 3, and 'right' Tuna Queen)
--
-- Author: Cian Luck
-- Date: 16 Sep 2021
---------------------------------------------------------------------

WITH 

    -----------------------------------------------------------------
    -- vessel info
    -- only one vessel active as the Tuna Queen 9n 2018 (ssvid 352894000)
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
    -- voyages confidence 3
    -- which voyages ended in 2018?
    -----------------------------------------------------------------
    voyages_c3 AS(
        SELECT 
            ssvid, 
            trip_id,
            trip_end,
            trip_end_confidence,
            trip_end_visit_id,
            EXTRACT(year FROM trip_start) AS year
        FROM 
            `world-fishing-827.pipe_production_v20201001.proto_voyages_c3`
        WHERE 
            trip_end_confidence >= 3
            AND DATE(trip_end) BETWEEN '2018-01-01' AND '2018-12-31'
    ),

    -----------------------------------------------------------------
    -- Tuna Queen voyages
    -----------------------------------------------------------------
    vessel_voyages AS(
        SELECT *
        FROM vessel_info
        JOIN voyages_c3
            USING(ssvid, year)
    ),

    -----------------------------------------------------------------
    -- port events associated with these voyages
    -----------------------------------------------------------------
    events AS (
        SELECT 
            *
        FROM 
           `world-fishing-827.pipe_production_v20201001.proto_port_visits`
        WHERE  
            DATE(end_timestamp) BETWEEN '2018-01-01' AND '2018-12-31'
            AND visit_id IN (
                SELECT trip_end_visit_id
                FROM vessel_voyages
            )
    )

------------------------------------------------------------
-- Return events
------------------------------------------------------------
SELECT *
FROM events