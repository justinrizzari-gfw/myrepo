###########################################################
#QUERY: Fishing effort by MMSI 367650000 between
# 01 Mar 2017 and 05 Mar 2017
#
#Based on EXAMPLE QUERY: FISHING EFFORT by Tyler C
# https://github.com/GlobalFishingWatch/bigquery-documentation-wf827/blob/master/queries/examples/current/fishing_hours_by_position_v20200831.sql
#
# DESCRIPTION:
# This query demonstrates how to extract valid positions
# and calculate fishing hours. The key features of the
# query include the following:
-- 1) Filter to only include positions from good segments
-- 2) Identify "fishing" positions using neural net score or night loitering. Use gfw_research.pipe_vYYYYMMDD_fishing to restrict to likely fishing vessels
-- 3) Calculate fishing hours using neural net scores and hours

WITH

  ########################################
  # This subquery identifies good segments
  good_segments AS (
  SELECT
    seg_id
  FROM
    `gfw_research.pipe_v20190502_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short),

  ####################################################################
  # Get the list of active fishing vessels that pass the noise filters
  vessel AS (
  SELECT
    ssvid, 
    ais_identity.shipname_mostcommon.value as shipname,
    best.best_flag,
    best.best_vessel_class as vessel_class,
    year
  FROM gfw_research.vi_ssvid_byyear_v20210706
  WHERE ssvid = '367650000'
  ),

  #####################################################################
  # This subquery fishing query gets all positions between 01 Mar 2017 and 05 Mar 2017
  # It queries the pipe_vYYYYMMDD_fishing table, which includes only likely
  # fishing vessels. 
  fishing AS (
      SELECT
      ssvid,
      lat,
      lon,
      nnet_score,
      timestamp,
      hours,
      night_loitering,
      # Get the year for filtering with the list of active fishing vessels
      EXTRACT(year from timestamp) as year,
    FROM
      # Query the pipe_vYYYYMMDD_fishing table since we're only
      # interested in likely fishing vessels. This reduces query size.
      `world-fishing-827.gfw_research.pipe_v20201001_fishing` 
    WHERE 
        DATE(_PARTITIONTIME) BETWEEN "2017-03-01" AND "2017-03-05" 
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
  JOIN vessel
  # Only keep positions for fishing vessels active that year
  USING(ssvid, year)
  ),

  ########################################################################
  # Use nnet_score to identify fishing activity for all vessel_class except
  # squid jiggers, for which we use night_loitering instead
  fishing_hours_filtered AS (
      SELECT 
      *,
      # Calculate fishing hours by evaluating neural net score
      # If the neural net score is >0.5, fishing hours are equal
      # to hours, else set fishing hours to 0
      CASE
        WHEN vessel_class = 'squid_jigger' and night_loitering = 1 THEN hours
        WHEN vessel_class != 'squid_jigger' and nnet_score > 0.5 THEN hours
      ELSE 0
    END
    AS fishing_hours
    FROM fishing_filtered 
  )



#####################
# Return fishing data
SELECT *
FROM fishing_hours_filtered