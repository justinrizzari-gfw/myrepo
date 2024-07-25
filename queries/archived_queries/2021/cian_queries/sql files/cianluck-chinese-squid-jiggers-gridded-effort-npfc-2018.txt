WITH

  ########################################
  # This subquery identifies good segments
  good_segments AS (
  SELECT
    seg_id
  FROM
    `gfw_research.pipe_v20201001_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short),

  ####################################################################
  # Get the list of active fishing vessels that pass the noise filters
  fishing_vessels AS (
  SELECT
    ssvid,
    year,
    best_flag,
    best_vessel_class
  FROM 
    `gfw_research.fishing_vessels_ssvid_v20210706`
  WHERE 
    best_flag = 'CHN' AND
    best_vessel_class = 'squid_jigger'
  ),
  
  #####################################################################
  # This subquery fishing query gets all fishing on November 20th, 2018
  # It queries the pipe_vYYYYMMDD_fishing table, which includes only likely
  # fishing vessels. However, we are not fully confident in all vessels on
  # this list and the table also includes noisy vessels. Thus, analyses
  # often filter the pipe_vYYYYMMDD_fishing table to a refined set of vessels
  fishing AS (
  SELECT
    ssvid,
    lat,
    lon,
    /*
    Assign lat/lon bins at desired resolution (here 10th degree)
    FLOOR takes the smallest integer after converting to units of
    0.1 degree - e.g. 37.42 becomes 374 10th degree units
    */
    FLOOR(lat * 10) as lat_bin,
    FLOOR(lon * 10) as lon_bin,
    EXTRACT(date FROM _partitiontime) as date,
    EXTRACT(year FROM _partitiontime) as year,
    hours,
    nnet_score,
    night_loitering
  /*
  Query the pipe_vYYYYMMDD_fishing table to reduce query
  size since we are only interested in fishing vessels
  */
  FROM
    `gfw_research.pipe_v20201001_fishing`
  # Restrict query to specific time range
  WHERE _partitiontime BETWEEN "2018-01-01" AND '2018-12-31'
  # Use good_segments subquery to only include positions from good segments
  AND seg_id IN (
    SELECT
      seg_id
    FROM
      good_segments)),


  ########################################################################
  # Filter fishing to just the list of active fishing vessels in that year
  fishing_filtered AS (
  SELECT *
  FROM fishing
  JOIN fishing_vessels
  # Only keep positions for fishing vessels active that year
  USING(ssvid, year)
  ),

   ########################################################################
  # Read in shapefile of npfc
  npfc AS (
    SELECT
        ST_GEOGFROMTEXT(string_field_1) AS polygon
    FROM
        `world-fishing-827.ocean_shapefiles_all_purpose.NPFC_shape`
    ),

  ########################################################################
  # Filter fishing to only include activity within the npfc
  fishing_in_npfc AS (
    SELECT
      *
    FROM
      fishing_filtered  
    WHERE
    IF
      (ST_CONTAINS( (
          SELECT
            polygon
          FROM
            npfc),
          ST_GEOGPOINT(lon, lat)),
        TRUE,
        FALSE)
    ),

  ########################################################################
  # Create fishing_hours attribute. Use night_loitering instead of nnet_score as indicator of fishing for squid jiggers
  fishing_hours_filtered AS (
  SELECT *,
    CASE
      WHEN best_vessel_class = 'squid_jigger' and night_loitering = 1 THEN hours
      WHEN best_vessel_class != 'squid_jigger' and nnet_score > 0.5 THEN hours
      ELSE 0
    END
    AS fishing_hours
  FROM fishing_in_npfc 
  ),

  #####################################################################
  # This subquery sums fishing hours and converts coordinates back to
  # decimal degrees
  fishing_binned AS (
  SELECT
    date,
    /*
    Convert lat/lon bins to units of degrees from 10th of degrees.
    374 now becomes 37.4 instead of the original 37.42
    */
    lat_bin / 10 as lat_bin,
    lon_bin / 10 as lon_bin,
    best_vessel_class,
    best_flag,
    SUM(hours) as hours,
    SUM(fishing_hours) as fishing_hours
  FROM fishing_hours_filtered
  GROUP BY date, lat_bin, lon_bin, best_vessel_class, best_flag
  )

#####################
# Return fishing data
SELECT *
FROM fishing_binned 