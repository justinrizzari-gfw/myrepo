WITH
fishing_trip_start as(
SELECT
trip_id as Fishing_trip_id,
trip_start as Fishing_trip_start_enc
FROM
pipe_production_v20190502.voyages
WHERE
ssvid IN (
SELECT
SAFE_CAST(neighbor_ssvid as string)
FROM
`scratch_luca.encounters_voyages_NPFC_panama`)),

clean_updated_encounters as (
SELECT
*,
-------------
--HANNAH NOTE: The code below is fine, but its important to keep in mind that because we are using the previous table to identify the 'last fish encounter'
--it will only show the last fishin encounter within 2020 inside NPFC, so if a Panama carrier had an encounter in december 2019 just outside of NPFC and then an ---encounter 3 days later inside NPFC in January 2020 it won't identify that previous encounter, it will just be 'NULL' or identify a different encounter as its ----last one. So, ideally you would expand the encounters previously to this before identify the 'last_fish_encounter'
--------------
LAG(event_start, 1) OVER (PARTITION BY SAFE_CAST(neighbor_ssvid as string) ORDER BY event_start ASC) AS last_fish_encounter
FROM
`scratch_luca.encounters_voyages_NPFC_panama` 
LEFT JOIN fishing_trip_start using (Fishing_trip_id)
ORDER BY
neighbor_ssvid,
event_start),


good_segments AS (
  SELECT
    seg_id
  FROM
    `gfw_research.pipe_v20200805_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short),


fishing_events_ssvid AS (SELECT *
FROM (
SELECT
DISTINCT
*,
FROM(
SELECT DISTINCT
      ssvid,
      lat,
      lon,
      nnet_score2,
      timestamp,
      hours,
IF(nnet_score2 > 0.5, hours, 0) as fishing_hours,
FROM
---HANNAH NOTE: CRITICAL TO use the correct fishing table, meaning it should be from the pipe version used for everything else. In this case pipe_v20190502
      `gfw_research.pipe_v20190502_fishing` 
WHERE
timestamp>=timestamp("2020-01-01")
AND
timestamp<=timestamp("2020-12-31")
AND
seg_id IN (
      SELECT
        seg_id
      FROM
        good_segments)
AND
ssvid IN (
SELECT
SAFE_CAST(neighbor_ssvid as string)
FROM
clean_updated_encounters)
))),


clean_fishing_data as(
SELECT
*,
CASE WHEN eez is null then "High Seas" else "EEZ" end as eez_area
FROM(
SELECT
*,
ROUND(FLOOR(lon/0.5) * 0.5, 2) as lon_bin2,
ROUND(FLOOR(lat/0.5) * 0.5, 2) as lat_bin_2,
FROM
fishing_events_ssvid)a
LEFT JOIN (
SELECT
--gridcode,
CAST(REGEXP_EXTRACT(SPLIT(gridcode, "_")[OFFSET(0)], r"-?\d*\.\d+") AS FLOAT64) AS eez_lon,
CAST(REGEXP_EXTRACT(SPLIT(gridcode, "_")[OFFSET(1)], r"-?\d*\.\d+") AS FLOAT64) AS eez_lat,
eez
FROM
---HANNAH NOTE: most recent version of spatial measures table
`pipe_static.spatial_measures_20201105` 
---------
-----HANNAH NOTE: important note:regions.eez is a simple Array, and “Null” means fishing high seas (it’s a valid entry) so we do need to keep via LEFT JOIN UNNEST.
-----------
LEFT JOIN UNNEST(regions.eez) as eez) b
ON a.lat_bin_2 = b.eez_lat 
AND a.lon_bin2 = b.eez_lon)

SELECT
SUM(fishing_hours) as fishing_hours,
eez_area,
FROM(
SELECT
*
FROM
(SELECT
*
FROM
clean_updated_encounters) a
JOIN (SELECT
* 
FROM 
clean_fishing_data ) b
ON SAFE_CAST(a.neighbor_ssvid as string) = b.ssvid
WHERE timestamp < event_start
AND
---HANNAH NOTE: just as an aside, correct use of parenthesis here is CRITICAL, when using and with OR statements, you will not produce the right result unless you ---ensure you use paraenthesis to produce the correct order of logic and test to make sure you are getting what you want to get.
---In addition, in this case the query is saying that you want fishing that occured since the start of the voyage that the vessel is on when it had the encounter, ---but before the encounter occured, in the last 3 weeks, and AFTER the last encounter if there was one. This is to try and help ensure that if we are saying -------fishing may be associated with a given transshipment, that it is correctly associated with the transshipment of interest and we aren't over estimating the -------associated fishing. These restraints are up to the analyst and knowledge regarding the activity.
(TIMESTAMP_DIFF(event_start,timestamp,DAY)<=21
AND
(timestamp > last_fish_encounter
OR last_fish_encounter IS NULL)
AND
timestamp > Fishing_trip_start)
)
GROUP BY
eez_area
