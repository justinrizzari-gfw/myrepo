WITH

rfmo as(
  SELECT
 
 SAFE.st_GeogFromText(string_field_1) AS grid_outline,

  FROM
  `world-fishing-827.ocean_shapefiles_all_purpose.NPFC_shape`),

  good_segments AS (
  SELECT
    seg_id
  FROM
    `pipe_production_v20201001.research_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short),

  track AS (
      SELECT
      ssvid,
      lat,
      lon,
      nnet_score,
      timestamp,
      hours,
    FLOOR(lat * 10) as lat_bin,
    FLOOR(lon * 10) as lon_bin,
    EXTRACT(date FROM _partitiontime) as date,
    EXTRACT(year FROM _partitiontime) as year,
    IF(nnet_score > 0.5, hours, 0) as fishing_hours
    FROM
      `pipe_production_v20201001.research_messages`
    WHERE _partitiontime >= '2018-01-01'
    AND
    _partitiontime <= '2018-12-31'
    
    and seg_id IN (
      SELECT
        seg_id
      FROM
        good_segments) and is_fishing_vessel=TRUE
        ),


fishing_filtered AS (
  SELECT
  ssvid,
    fishing_hours,
    date,
    nnet_score,
    lat_bin,
    lon_bin,
   grid_outline,

  FROM(
  SELECT
  *,
    ST_CONTAINS(grid_outline,
                ST_GeogPoint(lon,
                             lat)) as fra_loc
  FROM track a
   CROSS JOIN
      rfmo b)
  WHERE
  fra_loc = TRUE),


  

vi as(
SELECT
ssvid as vi_ssvid,
ais_identity.n_shipname_mostcommon.value as shipname,
ais_identity.n_imo_mostcommon.value as imo,
best.best_flag as flag,
best.best_vessel_class as gear
FROM
`gfw_research.vi_ssvid_v20230401`
where best.best_flag ='CHN'
) ,

long_filtered AS (SELECT
*
FROM(
SELECT
ssvid,
lat_bin,
lon_bin,
fishing_hours

FROM fishing_filtered)a
LEFT JOIN(
SELECT
*
FROM
vi
)b
ON
a.ssvid=b.vi_ssvid
WHERE gear='squid_jigger'),

  fishing_binned AS (
  SELECT
  flag,
    lat_bin / 10 as lat_bin,
    lon_bin / 10 as lon_bin,
    SUM(fishing_hours) as fishing_hours
  FROM long_filtered
    WHERE fishing_hours>0
  GROUP BY lat_bin, lon_bin,
  flag
  )
SELECT *,
    fishing_hours / (COS(udfs.radians(lat_bin)) * (111/100) * (111/100) )as fishing_hours_sq_km
FROM fishing_binned