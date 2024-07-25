
-- first get some info about the CHITOSE in 2021 (need vessel_id for encounters)
-- SELECT * FROM `pipe_production_v20201001.all_vessels_byyear_v2_v20231201` 
-- WHERE shipname IN ("CHITOSE") AND year = 2021

--------------------------------------------------------------
-- next pull all encounter ids where the the chitose was either vessel 1 or 2
-- this is in 2 steps, first all vessel ids in column 1, then all vessel ids in column 2
--------------------------------------------------------------
WITH 
vessel_1 AS(
  SELECT 
    encounter_id,
    vessel_1_id AS vessel_id,
    start_time,
    end_time
  FROM 
    `world-fishing-827.pipe_ais_v3_alpha_published.encounters`
  WHERE 
    start_time BETWEEN TIMESTAMP('2021-01-01 00:00:00 UTC') AND TIMESTAMP('2021-06-30 23:59:59 UTC') AND
    (vessel_1_id IN("bf272927e-ed20-22e9-4e26-b5e612e0df85") OR
    vessel_2_id IN("bf272927e-ed20-22e9-4e26-b5e612e0df85"))
),

vessel_2 AS(
  SELECT 
    encounter_id,
    vessel_2_id AS vessel_id,
    start_time,
    end_time
  FROM 
    `world-fishing-827.pipe_ais_v3_alpha_published.encounters`
  WHERE 
    start_time BETWEEN TIMESTAMP('2021-01-01 00:00:00 UTC') AND TIMESTAMP('2021-06-30 23:59:59 UTC') AND
    (vessel_1_id IN("bf272927e-ed20-22e9-4e26-b5e612e0df85") OR
    vessel_2_id IN("bf272927e-ed20-22e9-4e26-b5e612e0df85"))
),

--------------------------------------------------------------
-- next union the vessel ids that are NOT chitose 
-- to get full list of encountered vessel ids in a single column
--------------------------------------------------------------
all_encounters AS(
  SELECT 
    *
  FROM
    vessel_1
  WHERE vessel_id NOT IN ("bf272927e-ed20-22e9-4e26-b5e612e0df85")
  
  UNION ALL

  SELECT 
    *
  FROM
    vessel_2
  WHERE vessel_id NOT IN ("bf272927e-ed20-22e9-4e26-b5e612e0df85")
),

--------------------------------------------------------------
-- join vessel info based on vessel_id, and filter for 'best' fishing vessels
--------------------------------------------------------------
fishing_vessels AS(
  SELECT * FROM all_encounters
  LEFT JOIN (
  SELECT 
    vessel_id,
    ssvid,
    shipname,
    callsign,
    imo,
    gfw_best_flag,
    best_vessel_class,
    on_fishing_list_best
  FROM `world-fishing-827.pipe_production_v20201001.all_vessels_byyear_v2_v20231201`
  WHERE 
    year = 2021 
  ) AS all_vessels
  ON all_encounters.vessel_id = all_vessels.vessel_id
  WHERE
    on_fishing_list_best

) select * from fishing_vessels