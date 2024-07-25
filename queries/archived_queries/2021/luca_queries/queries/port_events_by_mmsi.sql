WITH 
port_events AS (
SELECT  
visit_id,
vessel_id,
ssvid,
start_timestamp,
end_timestamp,
duration_hrs,
confidence,
e.timestamp as event_timestamp,
e.vessel_lat as event_lat,
e.vessel_lon as event_lon,
e.anchorage_id as anchorage_id,
e.event_type as event_type
FROM `pipe_production_v20201001.proto_port_visits`
LEFT JOIN UNNEST (events) e
WHERE start_timestamp BETWEEN timestamp("2018-01-01")
AND timestamp("2018-01-31")
AND confidence>=3),
------------------------------------------------------------
-- Mapping vessel_id to SSVID
------------------------------------------------------------
anchorages AS (
SELECT
  *
FROM
  `world-fishing-827.gfw_research.named_anchorages`),
--------------------
-- Filter for Anchorages in Ivory Coast
--------------------
civ_anchorages AS (
SELECT
  *
FROM
  anchorages

)
------
SELECT
* EXCEPT (s2id)
FROM(
SELECT
visit_id,
vessel_id,
ssvid,
start_timestamp,
end_timestamp,
duration_hrs,
confidence,
event_timestamp,
event_lat,
event_lon,
anchorage_id,
event_type
FROM
port_events)a
JOIN(
SELECT
s2id,
label as end_label,
sublabel as end_sublabel,
iso3 as end_iso3,
distance_from_shore_m as end_dist_from_shore_m,
at_dock as end_at_dock
FROM
civ_anchorages)b
ON
a.anchorage_id=b.s2id
WHERE event_timestamp between '2018-01-19 14:00:18 UTC' and '2018-01-20 08:19:28 UTC'
AND ssvid='352894000'