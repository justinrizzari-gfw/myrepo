-- grab vessel locations for Q1

SELECT
    ssvid,
    timestamp,
    extract(date from _partitiontime) as date,
    lat,
    lon,
    -1 * elevation_m AS depth_m,
    distance_from_shore_m/1000 AS distance_from_shore_km,
    speed_knots,
    nnet_score
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
    # Select data range for track using date from partition to make query cheaper
  EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-10-24' AND '2017-11-06'
  # MMSI to get tracks for
  AND ssvid IN ("352894000") 