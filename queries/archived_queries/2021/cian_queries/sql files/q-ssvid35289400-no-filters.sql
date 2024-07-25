############################################################
# QUERY - Plot track of MMSI 352894000 from Oct 24th 2017 -
# Nov 6th 2017 - NO NOISE FILTERS
#
# Author: Cian Luck
# Date: 22/07/2021
############################################################

 
#######################################################
# This subquery gets all activity between 24th Oct 2017
# and 6th Nov 2017 - note that this is a non-fishing 
# vessel - not interested in fishing effort
SELECT 
    ssvid,
    lat,
    lon,
    timestamp,
FROM 
# Have to query the pipe_vYYYYMMDD table since we're interested
# in a non-fishing/carrier vessel. 
`world-fishing-827.gfw_research.pipe_v20201001` 
WHERE 
    DATE(_PARTITIONTIME) BETWEEN '2017-10-24' AND '2017-11-06'
    AND ssvid = '352894000'