-------------------------------------------------------------
-- Query: What ports were visited by carriers in IOTC in 2020
-- after loitering events? How many port visits occurred at
-- each port?
--
-- Author: Cian Luck
-- Date: 16 Sep 2021
-------------------------------------------------------------

-- First task: pull loitering events
-- Second: id ports
-- Third: visits per port


WITH
    --------------------------------------------------------
    -- Restrict to carrier vessels using the vessel database
    --------------------------------------------------------
    carrier_vessels as(
        SELECT 
            mmsi AS ssvid,
            year,
            flag,
            vessel_class
        FROM 
            `vessel_database.carrier_vessels_byyear_v20210701`
        WHERE
            year = 2020
    ),

    --------------------------------------------------------
    -- Load shapefile of IOTC
    --------------------------------------------------------
    iotc AS (
        SELECT
        -- note: had to add make_valid => TRUE as seems to be a problem with the shapefile
            ST_GEOGFROMTEXT(string_field_1, make_valid => TRUE) AS polygon 
        FROM
            `world-fishing-827.ocean_shapefiles_all_purpose.IOTC_shape_feb2021`
    ), 

    --------------------------------------------------------    
    -- Filter loitering events to those that are at least 20-nm from shore
    -- and are loitering for at least 4 hours
    -- Also filter for good segments that are not overlapping and short using gfw_research.pipe_v_segs
    -- Adjust desired timeframe
    -- Filter only locations within IOTC
    --------------------------------------------------------
    loitering AS(
        SELECT
            ssvid,
            seg_id,
            loitering_start_timestamp,
            loitering_end_timestamp,
            loitering_hours,
        FROM
            `pipe_production_v20201001.loitering`, iotc
        WHERE
            avg_distance_from_shore_nm >= 20
            AND loitering_hours >= 4
            AND loitering_hours < 24
            AND avg_speed_knots < 2
            AND seg_id IN (
                SELECT
                    seg_id
                FROM
                    `gfw_research.pipe_v20201001_segs`
                WHERE
                    good_seg
                    AND NOT overlapping_and_short)
            AND loitering_start_timestamp >= TIMESTAMP('2020-01-01')
            AND loitering_end_timestamp <= TIMESTAMP('2020-12-31')
            AND ST_CONTAINS(iotc.polygon, ST_GEOGPOINT(start_lon, start_lat))
            AND ssvid IN (
                SELECT
                    ssvid
                FROM
                    carrier_vessels)
    ),

    --------------------------------------------------------
    -- Join loitering_events
    -- This is what I would return for a loitering query
    -------------------------------------------------------- 
    loitering_events AS(
        SELECT
            ssvid,
            seg_id,
            flag,
            loitering_start_timestamp,
            loitering_end_timestamp,
            loitering_hours,
        FROM
            loitering
        LEFT JOIN
            carrier_vessels
        USING
            (ssvid)
        ORDER BY
            ssvid,
            seg_id,
            loitering_start_timestamp
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
            trip_end_confidence,
            trip_end_visit_id,
            EXTRACT(year FROM trip_start) AS year
        FROM 
            `world-fishing-827.pipe_production_v20201001.proto_voyages_c4`
        WHERE 
            trip_end_confidence >= 4
            AND DATE(trip_end) BETWEEN '2020-01-01' AND '2020-12-31'
    ),

    -----------------------------------------------------------------
    -- join loitering and voyages_c4
    -- conditional trip_end >= loitering_end_timestamp
    -----------------------------------------------------------------
    voyages_post_loitering AS(
        SELECT 
            *
        FROM(
            SELECT * 
            FROM voyages_c4  
        ) a            
        JOIN (
            SELECT
                ssvid,
                loitering_end_timestamp,
                loitering_start_timestamp
            FROM loitering_events 
        ) b
            ON a.ssvid = b.ssvid
            AND a.trip_end >= b.loitering_end_timestamp
            AND a.trip_start <= b.loitering_start_timestamp
    ),

    -----------------------------------------------------------------
    -- keep only the minimum date post each loitering event
    -----------------------------------------------------------------
    voyages_post_loitering_min AS(
        SELECT 
            v1.*
        FROM voyages_post_loitering v1
        JOIN (
            SELECT 
                trip_id,
                MIN(trip_end) AS trip_end
            FROM voyages_post_loitering 
            GROUP BY trip_id
        ) v2
            USING(trip_id, trip_end)
    ),

    -----------------------------------------------------------------
    -- port visits associated with these voyages
    -----------------------------------------------------------------
    visits AS (
        SELECT 
            *
        FROM 
           `world-fishing-827.pipe_production_v20201001.proto_port_visits`
        WHERE  
            DATE(end_timestamp) BETWEEN '2020-01-01' AND '2020-12-31'
            AND visit_id IN (
                SELECT trip_end_visit_id
                FROM voyages_post_loitering_min
            )
    ),

    -----------------------------------------------------------------
    -- count visits per port
    -----------------------------------------------------------------
    visits_sum AS(
        SELECT 
            end_anchorage_id,
            COUNT(DISTINCT visit_id) AS n_visits
        FROM visits
        WHERE confidence >= 4
        GROUP BY end_anchorage_id
        ORDER BY n_visits DESC
    )





-------------------------------------------------------------
-- Return
-------------------------------------------------------------
SELECT *
FROM visits_sum