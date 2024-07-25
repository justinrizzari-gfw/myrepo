-- 10 Nov 2023
-- MS 
-- transhipment inspection q1 
-- How many encounter events did CHITOSE have with fishing vessels in the first six months of 2021?


-- set variables for table query
CREATE TEMP FUNCTION name() AS ('CHITOSE');
CREATE TEMP FUNCTION start_date() AS (DATE('2021-01-01'));
CREATE TEMP FUNCTION end_date() AS (DATE('2021-06-30'));


WITH 

-- Get vessel info for all vessels
vessel_info AS (
  SELECT
    vessel_id, 
    ssvid, 
    first_timestamp,
    last_timestamp,
  FROM
    `world-fishing-827.pipe_ais_v3_alpha_published.vessel_info`
  WHERE
    DATE(first_timestamp) <= end_date()
    AND DATE(last_timestamp) >= start_date() 
    ),

-- try test the identity_all_vessels_v table as an alternate 
-- works well and nice flat table 
-- missing vessel_id so will join that on 
-- left join not working properly. moving on 
identity_core_carrier AS (
  SELECT
    ic.ssvid,
    vi.vessel_id,
    n_shipname, 
    n_callsign,
    imo,
    flag, 
    ic.first_timestamp,
    ic.last_timestamp,
    geartype
  FROM
    `world-fishing-827.pipe_ais_v3_alpha_published.identity_core_v20231001` ic
  LEFT JOIN (
      SELECT 
        *
      FROM vessel_info
    ) AS vi ON (
      ic.ssvid = vi.ssvid 
      AND ic.first_timestamp >= vi.first_timestamp 
      AND ic.last_timestamp <= vi.last_timestamp
    ) 
  WHERE
    n_shipname = name()
    AND DATE(ic.first_timestamp) < end_date()
    AND DATE(ic.last_timestamp) > start_date() 
    AND is_carrier = TRUE
    ),

identity_core_fv AS (
  SELECT
    ic.ssvid,
    vi.vessel_id,
    n_shipname, 
    n_callsign,
    imo,
    flag, 
    ic.first_timestamp,
    ic.last_timestamp,
    geartype
  FROM
    `world-fishing-827.pipe_ais_v3_alpha_published.identity_core_v20231001` ic
  LEFT JOIN (
      SELECT 
        *
      FROM vessel_info
    ) AS vi ON (
      ic.ssvid = vi.ssvid 
      AND ic.first_timestamp >= vi.first_timestamp 
      AND ic.last_timestamp <= vi.last_timestamp
    ) 
  WHERE
    DATE(ic.first_timestamp) < end_date()
    AND DATE(ic.last_timestamp) > start_date() 
    AND is_fishing = TRUE
    ),

-- now lets look at encounters
encounters AS (
  SELECT 
    * 
  FROM `world-fishing-827.pipe_ais_v3_alpha_published.encounters` 
  WHERE   
      (vessel_1_id IN (SELECT vessel_id FROM identity_core_carrier) 
      AND vessel_2_id IN (SELECT vessel_id FROM identity_core_fv)) 
    OR 
      (vessel_1_id IN (SELECT vessel_id FROM identity_core_fv) 
      AND vessel_2_id IN (SELECT vessel_id FROM identity_core_carrier))

)

SELECT 
  * 
FROM encounters
WHERE 
  start_time < timestamp(end_date())
  AND end_time >= timestamp(start_date())