###########################################################
#QUERY: Fishing effort by MMSI 432346000 between
# 1st Jan 2017 and 10 Feb 2017
#
#Based on EXAMPLE QUERY: FISHING EFFORT by Tyler C
#
# DESCRIPTION:
# This query demonstrates how to extract valid positions
# and calculate fishing hours. The key features of the
# query include the following:
-- 1) Filter to only include positions from good segments
-- 2) Identify "fishing" positions using neural net score. Use gfw_research.pipe_vYYYYMMDD_fishing to restrict to likely fishing vessels
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
    year
  FROM gfw_research.vi_ssvid_byyear_v20210706
  WHERE ssvid = '432346000'
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
      nnet_score,
      timestamp,
      hours,
      # Get the year for filtering with the list of active fishing vessels
      EXTRACT(year from timestamp) as year,
      # Calculate fishing hours by evaluating neural net score
      # If the neural net score is >0.5, fishing hours are equal
      # to hours, else set fishing hours to 0
      IF(nnet_score > 0.5, hours, 0) as fishing_hours
    FROM
      # Query the pipe_vYYYYMMDD_fishing table since we're only
      # interested in likely fishing vessels. This reduces query size.
      `world-fishing-827.gfw_research.pipe_v20201001_fishing` 
    WHERE 
             DATE(_PARTITIONTIME) BETWEEN "2017-01-01" AND "2017-02-10" 
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
  )

#####################
# Return fishing data
SELECT *
FROM fishing_filtered