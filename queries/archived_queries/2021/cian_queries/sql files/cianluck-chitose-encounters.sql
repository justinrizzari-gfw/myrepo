-----------------------------------------------
-- Query: How many encounter events did CHITOSE have 
-- with fishing vessels in the first six months of
-- 2021
-----------------------------------------------

-- Cian Luck, Updated 23 Aug 2021

-- Based on example query:
#standardSQL
  -- Matching Encounter data to SSVID values
  -- Hannah Linder, Updated August 31,2020
  --
  -- This query can be used to get ssvid values (generally called MMSIs) matched to the encounter data, specifing both time and lat/lon range
  -- Then this query places the two encounter vessels on the same row of data for easy data management


-----------------------------------------------
-- Specify date range
-----------------------------------------------
---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2021-01-01'));

---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2021-06-30'));

WITH

-----------------------------------------------
-- vessel info
-----------------------------------------------
    vessel_info_chitose AS (
    SELECT 
        ssvid,
        year,
        ais_identity.n_shipname_mostcommon.value AS shipname,
        ais_identity.n_shipname_mostcommon.count AS shipname_count,
        best.best_flag AS flag,
        best.best_vessel_class AS vessel_class,
        on_fishing_list_best
    FROM 
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
    WHERE 
        'CHITOSE' IN (ais_identity.n_shipname_mostcommon.value)
        AND year = 2021),

-----------------------------------------------
 -- Retrieve initial encounter data, specifing time range and lat/lon
 -- JSON_EXTRACT is used to seperate the listed event_info data of interest into separate columns
 -----------------------------------------------
    encounters AS (
    SELECT
        event_id,
        vessel_id,
        event_start,
        event_end,
        lat_mean,
        lon_mean,
        JSON_EXTRACT(event_info,
        "$.median_distance_km") AS median_distance_km,
        JSON_EXTRACT(event_info,
        "$.median_speed_knots") AS median_speed_knots,
        -- extract the ssvid for both vessels involved in encounter
        JSON_VALUE(event_vessels,
        '$[0].ssvid') AS ssvid_a,
        JSON_VALUE(event_vessels,
        '$[1].ssvid') AS ssvid_b,
    SPLIT(event_id, ".")[ORDINAL(1)] AS event,
    CAST (event_start AS DATE) event_date,
    EXTRACT(YEAR FROM event_start) AS year
    FROM
    `world-fishing-827.pipe_production_v20201001.published_events_encounters`
    WHERE
        DATE(event_start) >= minimum()
        AND DATE(event_end) <= maximum()
        AND lat_mean < 90
        AND lat_mean > -90
        AND lon_mean < 180
        AND lon_mean > -180),

-----------------------------------------------
-- grab daily information on ssvid corresponding to vessel_id 
-----------------------------------------------
    ssvid_map AS (
    SELECT
        vessel_id,
        ssvid
    FROM
        `world-fishing-827.pipe_production_v20201001.vessel_info`),

-----------------------------------------------
-- encounters with ssvid
-----------------------------------------------
 -- Join the encounters data with the ssvid data on the same vessel_id and event day to ensure correct SSVID
    encounter_ssvid AS (
    SELECT * EXCEPT(vessel_id)
    FROM (
    SELECT
    *
    FROM
    encounters) a
    JOIN (
    SELECT *
    FROM
    ssvid_map) b
    ON a.vessel_id = b.vessel_id),

-----------------------------------------------
-- filter encounters by ssvid in vessel_info
-----------------------------------------------
    encounter_ssvid_filtered AS (
    SELECT *
    FROM encounter_ssvid 
    JOIN vessel_info_chitose
        USING(ssvid, year)
    ),

-----------------------------------------------
-- append vessel class info for both encounter vessels
-----------------------------------------------
    vessel_info_all AS (
    SELECT 
        ssvid,
        best.best_vessel_class AS vessel_class,
        on_fishing_list_best
    FROM 
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
    WHERE year = 2021
    ),

    -- add vessel info for vessel a
    encounter_ssvid_a AS(
    SELECT * 
    FROM (
        SELECT *
        FROM
        encounter_ssvid_filtered ) a
    JOIN (
        SELECT
        ssvid, 
            vessel_class AS vessel_class_a,
            on_fishing_list_best AS on_fishing_list_best_a
        FROM
        vessel_info_all) b
    ON a.ssvid_a = b.ssvid),

    -- add vessel info for vessel b
    -- add vessel info for vessel a
    encounter_ssvid_b AS(
    SELECT * 
    FROM (
        SELECT *
        FROM
        encounter_ssvid_a) a
    JOIN (
        SELECT
            ssvid, 
            vessel_class AS vessel_class_b,
            on_fishing_list_best AS on_fishing_list_best_b
        FROM
        vessel_info_all) b
    ON a.ssvid_b = b.ssvid)


-----------------------------------------------
-- return encounter_ssvid_b
-----------------------------------------------
SELECT
*
FROM
encounter_ssvid_b