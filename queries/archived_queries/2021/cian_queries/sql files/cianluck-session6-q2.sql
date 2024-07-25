-------------------------------------------------------------
-- How many encounters occurred in NPFC between trawlers and 
-- carrier vessels in the last three months 
-- (July - September 2021)?
-------------------------------------------------------------

-----------------------------------------------
-- Specify date range
-----------------------------------------------
---SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (DATE('2021-01-01'));

---SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (DATE('2021-09-30'));

WITH

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
        '$.median_distance_km') AS median_distance_km,
        JSON_EXTRACT(event_info,
        '$.median_speed_knots') AS median_speed_knots,
        -- extract the ssvid for both vessels involved in encounter
        JSON_VALUE(event_vessels,'$[0].ssvid') AS ssvid_a,
        JSON_VALUE(event_vessels,'$[1].ssvid') AS ssvid_b,
    SPLIT(event_id, '.')[ORDINAL(1)] AS event,
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
-- create a list of carrier vessels
-----------------------------------------------
    carrier_vessels AS (
    SELECT
        identity.ssvid AS ssvid
    FROM
        `vessel_database.all_vessels_v20210601`
    LEFT JOIN
        UNNEST(registry)
        -- LEFT JOIN
        --  UNNEST(activity)
    WHERE
        is_carrier
        AND confidence >= 3),

-----------------------------------------------
-- trawlers
-----------------------------------------------
    trawlers AS (
    SELECT 
        ssvid,
        year,
        ais_identity.shipname_mostcommon.value as shipname
    FROM 
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210706`
    WHERE 
        best.best_vessel_class = 'trawlers'
        AND year = 2021),

-----------------------------------------------
-- keep only trawlers and carriers
-----------------------------------------------
    encounters_trawlers_carriers AS(
        SELECT *
        FROM(
            SELECT 
                *,
                -- try creating a vessel_class column (carrier or trawler) for each vessel
                CASE
                    WHEN ssvid_a IN (SELECT ssvid FROM carrier_vessels) THEN 'carrier'
                    WHEN ssvid_a IN (SELECT ssvid FROM trawlers) THEN 'trawler'
                ELSE 'NA'
                END
                AS vessel_class_a,
                CASE
                    WHEN ssvid_b IN (SELECT ssvid FROM carrier_vessels) THEN 'carrier'
                    WHEN ssvid_b IN (SELECT ssvid FROM trawlers) THEN 'trawler'
                ELSE 'NA'
                END
                AS vessel_class_b
            FROM 
                encounter_ssvid
        )
        WHERE 
            -- (vessel_class_a = 'carrier' AND vessel_class_b = 'trawler') 
            -- OR (vessel_class_b = 'carrier' AND vessel_class_a = 'trawler')
            vessel_class_a != 'NA'
            OR vessel_class_b != 'NA'
    ),

-----------------------------------------------
-- read in shapefile of NPFC
-----------------------------------------------
    npfc AS (
        SELECT
            ST_GEOGFROMTEXT(string_field_1) AS polygon
        FROM
            `world-fishing-827.ocean_shapefiles_all_purpose.NPFC_shape`
        ),

------------------------------------------------
-- Filter fishing to only include activity within the npfc
------------------------------------------------
  encounters_in_npfc AS (
    SELECT
      *
    FROM
      encounters_trawlers_carriers   
    WHERE
    IF
      (ST_CONTAINS( (
          SELECT
            polygon
          FROM
            npfc),
          ST_GEOGPOINT(lon_mean, lat_mean)),
        TRUE,
        FALSE)
    ),

------------------------------------------------
-- Which carriers were authorized within the NPFC
------------------------------------------------
init_npfc_auth as ( 
    SELECT
  *
  FROM (
  SELECT
  identity.ssvid as auth_ssvid,
  identity.n_shipname as auth_shipname,
  identity.imo as auth_imo,
  identity.flag as auth_flag,
  authorized_to as registry_authorized_to,
  authorized_from as registry_authorized_from,
  is_active as registry_active,
  udfs.extract_regcode (list_uvi) AS reg,
  first_timestamp,
  last_timestamp
  FROM
  `vessel_database.all_vessels_v20210701`
   LEFT JOIN UNNEST(registry)
   LEFT JOIN UNNEST(activity)
   LEFT JOIN UNNEST(feature.geartype)
   WHERE
----AIS record 'matches' to registry record based on identity information, specifying FALSE or loose_match TRUE here would allow me to see
------records where we are less confident that the registry is the correct match to the AIS data  
  matched
-----Pull any vessels that include registry records from IOTC, CCSBT, or TWN (twn can't join IOTC so it has its own registry)
  AND EXISTS (
    SELECT *
    FROM UNNEST (registry)
  --- because I'm using OR I put parenthesis around the OR function because it can cause order of operations confusion otherwise
    WHERE list_uvi LIKE 'NPFC%')
   AND
   --IMPORTANT to make sure that the time period the vessel was active on AIS was during the time period of authorization record
   --we want the vessel to be actively transmitting on AIS at some period during 2020
   DATE(first_timestamp)<=maximum()
   AND
   DATE(last_timestamp)>=minimum())
   WHERE
  --since we unnested the records and were looking for any records related to these rfmos, we may have gotten other entries from other RFMOS as well
  --that were nested in with these rfmos in the registry records merged with the AIS information. Therefore again here we specify we only want registry records
  --from these rfmos
   reg IN ('NPFC')),  

---------
--clean up authorization info and ensure only showing records that are registered during the time of AIS transmittion and period of interest
----------
authorization_info_clean as(
  SELECT
  auth_ssvid,
  authorized_to,
  authorized_from,
  reg,
  first_timestamp,
  last_timestamp
  FROM(
  SELECT
  auth_ssvid,
  auth_shipname,
  auth_imo,
  auth_flag,
  registry_authorized_to as authorized_to,
  registry_authorized_from authorized_from,
  reg,
  first_timestamp,
  last_timestamp
  FROM
  init_npfc_auth
  WHERE
--   we want the registry records that occur at some point in 2020
  DATE(registry_authorized_from)<=maximum()
  AND DATE(registry_authorized_to)>=minimum()
--   we want to make sure that there is overlap between the AIS transmission period and the registry records
  AND(
   registry_authorized_from > first_timestamp
   OR
   registry_authorized_to < last_timestamp
    )
    )
  GROUP BY
  auth_ssvid,
  authorized_to,
  authorized_from,
  reg,
  first_timestamp,
  last_timestamp
  ),

--Merge authorization records with carrier vessels during the time of loitering events
carrier_authorization as (
  SELECT
    *
   FROM(
    SELECT
        *
    FROM
    encounters_in_npfc)a
  --Join loitering data with the authorization ssvid, authorization time range, and registry name identified above. In the above query we already ensure the registry records we are pulling 
  --are specifically for the time range of interest when we are looking at the loitering (2020) and are correctly for when the vessel was transmitting AIS during the same time period
  LEFT JOIN(
    SELECT
    auth_ssvid,
    authorized_from as carrier_authorized_from,
    authorized_to as carrier_authorized_to,
    reg as carrier_reg
    FROM
    authorization_info_clean)b
  ON
    SAFE_CAST(a.ssvid as STRING)=SAFE_CAST(b.auth_ssvid as string)
  AND
    ---Merge the authorization records that occur during the period of the loitering event
    a.event_end>=b.carrier_authorized_from
    AND
    a.event_start<=b.carrier_authorized_to),

-----------------------------------------------
-- vessel names of trawlers
-----------------------------------------------
carrier_authorization_2 AS (
    SELECT 
        *,
        CASE
            WHEN vessel_class_a = 'trawler' THEN ssvid_a
            WHEN vessel_class_b = 'trawler' THEN ssvid_b
        ELSE 'NA'
        END
        AS trawler_ssvid
    FROM carrier_authorization
),

-----------------------------------------------
-- get trawler shipnames
-----------------------------------------------
carrier_authorization_trawler_name AS (
    SELECT 
        *
    FROM carrier_authorization_2
    LEFT JOIN (
        SELECT  
            ssvid,
            shipname AS trawler_shipname
        FROM 
            trawlers 
    ) b
    ON carrier_authorization_2.trawler_ssvid = b.ssvid
)

-----------------------------------------------
-- return carrier_authorization
-----------------------------------------------
SELECT
*
FROM
carrier_authorization_trawler_name