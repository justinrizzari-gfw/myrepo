---------------------------------------------------------------------
-- Query: How many port events occurred in the port visit associated 
-- with the voyage that ended in Zadar on 20 Jan 2018?
--
-- Author: Cian Luck
-- Date: 15 Oct 2021
---------------------------------------------------------------------

WITH 

    -----------------------------------------------------------------
    -- find anchorage called Zadar
    -----------------------------------------------------------------
    anchorage_zadar AS (
        SELECT 
            s2id,
            label,
            sublabel
        FROM 
            `world-fishing-827.gfw_research.named_anchorages`
        WHERE 
            label = 'ZADAR' 
            OR sublabel = 'ZADAR'
    ),

    -----------------------------------------------------------------
    -- voyage ending in Zadar on 20 Jan 2018
    -- this returns 14 voyages
    -----------------------------------------------------------------
    -- voyages AS (
    --     SELECT 
    --         *
    --     FROM 
    --        `world-fishing-827.gfw_research.voyages_no_overlapping_short_seg_v20210226`
    --     WHERE 
    --         DATE(trip_end) = '2018-01-20'
    --         AND trip_end_anchorage_id IN (
    --             SELECT s2id
    --             FROM anchorage_zadar
    --         )
    -- ),

    voyages AS (
        SELECT 
            *
        FROM 
           `world-fishing-827.pipe_production_v20201001.proto_voyages_c4`
        WHERE 
            DATE(trip_end) = '2018-01-20'
            AND trip_end_confidence >= 4
            AND trip_end_anchorage_id IN (
                SELECT s2id
                FROM anchorage_zadar
            )
    ),

    -----------------------------------------------------------------
    -- port events associated with these voyages
    -----------------------------------------------------------------
    events AS (
        SELECT 
            visit_id,
            events
        FROM 
           `world-fishing-827.pipe_production_v20201001.proto_port_visits`
        LEFT JOIN UNNEST(events)
        WHERE  
            DATE(end_timestamp) = '2018-01-20'
            AND visit_id IN (
                SELECT trip_end_visit_id
                FROM voyages
            )
    )

-----------------------------------------------------------------
-- return voyages
-----------------------------------------------------------------
SELECT * 
FROM events