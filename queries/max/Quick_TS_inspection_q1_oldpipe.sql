---------------------------------------------------------------------------
-- Encounters between Chitose and FC in 2021

-- Author: Max Schofield
-- Date: 24 Nov 2023
---------------------------------------------------------------------------

---SET your dates of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2021-01-01'));
CREATE TEMP FUNCTION maximum() AS (DATE('2021-06-30'));


WITH

    ----------------------------------------------------------------------
    -- encounters
    ----------------------------------------------------------------------
    encounters AS (
        SELECT
            event_id,
            JSON_EXTRACT_SCALAR(event_vessels,
            "$[0].ssvid") as ssvid,
            vessel_id,
            event_start,
            event_end,
            lat_mean,
            lon_mean,
            JSON_EXTRACT(event_info,
            "$.median_distance_km") AS median_distance_km,
            JSON_EXTRACT(event_info,
            "$.median_speed_knots") AS median_speed_knots,
            SPLIT(event_id, ".")[ORDINAL(1)] AS event,
            CAST (event_start AS DATE) event_date,
            EXTRACT(YEAR FROM event_start) AS year
        FROM
            `world-fishing-827.pipe_production_v20201001.published_events_encounters`
        WHERE
            DATE(event_start) BETWEEN minimum() AND maximum()
            AND DATE(event_end) BETWEEN minimum() AND maximum()
            -- at least Xkm from port
             AND start_distance_from_port_km >= 10
             AND end_distance_from_port_km >= 10
            -- at least Xkm from shore
            AND start_distance_from_shore_km >= 10
            AND end_distance_from_shore_km >= 10
            ---The below restrictions are just a precaution to remove any encounters that may be noisey
            ---and produce odd coordinate locations outside the scope of the globe
            AND lat_mean < 90
            AND lat_mean > -90
            AND lon_mean < 180
            AND lon_mean > -180),

    ----------------------------------------------------------------------
    -- create curated carrier list
    -- using carriers identified in SLE through collab with TMT
    ----------------------------------------------------------------------


    bunker_vessels AS (
      SELECT
      DISTINCT
        ssvid AS carrier_ssvid,
        shipname AS carrier_shipname,
        -- imo AS bunker_imo,
        gfw_best_flag AS carrier_flag,
      FROM
      -- IMPORTANT: change below to most up to date table
      `world-fishing-827.pipe_production_v20201001.all_vessels_byyear_v2`   
      WHERE
        ssvid IN ('563418000')
        AND year = 2021),


    ----------------------------------------------------------------------
    -- all vessels of interest in SLE including carriers, bunkers and fv
    ---
    ----------------------------------------------------------------------

    fv_vessel_info AS (
      SELECT
        ssvid AS fv_ssvid, 
        registry_info.best_known_imo AS fv_imo, 
        registry_info.best_known_shipname AS fv_shipname,
        best.best_flag AS fv_flag,
        best.best_vessel_class AS fv_class 
      FROM
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220901`
      WHERE 
        on_fishing_list_best = TRUE
    ),

    ----------------------------------------------------------------------
    --Join vessel the bunker encountered
    --because there is one row per vessel (therefore two rows per encounter) just join again with the encounter dataset
    --on the unqiue event id but specify it must be the non-carrier ssvid this time
    ----------------------------------------------------------------------
    encounter_bunkers as(
        SELECT
        *
        FROM encounters
        INNER JOIN
            bunker_vessels
        ON
            encounters.ssvid = CAST(bunker_vessels.carrier_ssvid AS string)
            --AND EXTRACT(year FROM encounters.event_start) = carrier_vessels.year
            ),

    all_encounters as (
        SELECT
            carrier_ssvid,
            carrier_shipname, 
            neighbor_ssvid,
            event_start,
            event_end,
            lat_mean as mean_lat,
            lon_mean as mean_lon,
            median_distance_km,
            median_speed_knots,
            (TIMESTAMP_DIFF(event_end,event_start,minute)/60) event_duration_hr,
            a.event AS event,
            event_date
        FROM
        (
        SELECT
            *
        FROM
            encounter_bunkers) a
        JOIN
        (SELECT
            ssvid AS neighbor_ssvid,
            event
        FROM
            encounters) b
        ON a.event = b.event
        WHERE CAST(carrier_ssvid AS string) != neighbor_ssvid
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12),

    fv_sv_encs as(
        SELECT
            event,
            event_start,
            event_end,
            mean_lat,
            mean_lon,
            event_Duration_hr,
            median_speed_knots,
            carrier_ssvid,
            carrier_shipname,
            fv_ssvid,
            fv_shipname,
            fv_class,
            fv_flag,
        FROM all_encounters
        INNER JOIN
            fv_vessel_info
        ON
            all_encounters.neighbor_ssvid = fv_vessel_info.fv_ssvid
            --AND EXTRACT(year FROM all_encounters.event_start) = fishing_vessels.year
            )

----------------------------------------------------------------------
-- Return
----------------------------------------------------------------------
  SELECT
    DISTINCT event,
    *
  FROM
    fv_sv_encs  
    ORDER BY event_start


