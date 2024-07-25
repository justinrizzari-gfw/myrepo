-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

SELECT
    ssvid,
    seg_id,
    timestamp,
    lon,
    lat,
    hours,
    nnet_score
  FROM
    `pipe_production_v20201001.research_messages`
  WHERE
    ssvid = '352894000'
  AND EXTRACT(DATE FROM _partitiontime) BETWEEN '2017-10-24' AND '2017-11-06'
