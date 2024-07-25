### I used this code and the resulting tables to answer all questions in part 2 - Vessel Info

# Q1 What is the name, callsign, flag state, and imo of the vessel with MMSI 353154000 during 2018?
SELECT
ssvid,
year,
ais_identity,
inferred,
registry_info,
best,
on_fishing_list_known,
on_fishing_list_nn,
on_fishing_list_sr,
on_fishing_list_best
FROM
`world-fishing-827.gfw_research.vi_ssvid_byyear_v20190227` # This table includes annual best activity and identity information for the vessel based only on data in each year.
WHERE
ssvid = '353154000'
AND
year = 2018;
## A1 It has 5 shipnames in this year, the most common one is 'CABODEPALOS'. Flagged to Pananma. Best known IMO is 9550151, based on registry (note: name in registry is different)

#Q2 Is the above answer different if you pull from the vessel info table (`gfw_research.vi_ssvid_v`) versus the vessel registry table (`vessel_database.all_vessels_v`)?
SELECT
ssvid,
ais_identity,
inferred,
registry_info,
best,
on_fishing_list_known,
on_fishing_list_nn,
on_fishing_list_sr,
on_fishing_list_best
FROM
`world-fishing-827.gfw_research.vi_ssvid_v20230701` #This table includes the best activity and identity information available for the vessel based on its full AIS timeseries
WHERE
ssvid = '353154000';

SELECT
matched,
identity,
is_fishing,
is_carrier,
is_bunker,
is_new,
has_geartype_changed,
changed_geartypes,
registry
FROM
`world-fishing-827.vessel_database.all_vessels_v20230701` #This table contains all identity information for a vessel available from AIS and vessel registries, including where a vessel is authorized to operate, and who owns the vessel. So the AIS info is from vi_ssvid_v?
WHERE
identity.ssvid = '353154000';

SELECT
year,
matched,
identity,
is_fishing,
is_carrier,
is_bunker,
is_new,
has_geartype_changed,
changed_geartypes,
registry
FROM
`world-fishing-827.vessel_database.single_mmsi_matched_vessels_byyear_v20230701` #This table includes the most representative vessel per MMSI by year from the vessel database. Therefore, a single MMSI represents one row (vessel) in the given year in this table
WHERE
identity.ssvid = '353154000';
