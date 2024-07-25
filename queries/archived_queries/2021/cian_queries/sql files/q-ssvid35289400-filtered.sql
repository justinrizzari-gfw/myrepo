############################################################
# QUERY - Plot track of MMSI 352894000 from Oct 24th 2017 -
# Nov 6th 2017
#
# Apply noise filters and split tracks into appropriate segs
#
# Author: Cian Luck
# Date: 22/07/2021
############################################################
WITH
    #######################################################
    # This subquery pulls out seg_id and vessel_id for
    # time period of interest
    seg_info AS (
    SELECT 
        ssvid,
        vessel_id,
        seg_id,
    FROM `world-fishing-827.pipe_production_v20201001.segment_info` 
    WHERE 
        ssvid = '352894000' AND
        first_timestamp BETWEEN '2017-10-24 00:00:00 UTC' AND '2017-11-06 00:0:00 UTC'
        OR ssvid = '352894000' AND
        last_timestamp BETWEEN '2017-10-24 00:00:00 UTC' AND '2017-11-06 00:0:00 UTC'
    ),


    #######################################################
    # This subquery identifies good track segments
    good_segments AS (
    SELECT 
        seg_id
    FROM 
    `world-fishing-827.gfw_research.pipe_v20201001_segs` 
    WHERE 
        good_seg
        AND positions > 10
        AND NOT overlapping_and_short),


    #######################################################
    # This subquery gets all activity between 24th Oct 2017
    # and 6th Nov 2017 - note that this is a non-fishing 
    # vessel - not interested in fishing effort
    vessel_activity AS(
    SELECT 
        lat,
        lon,
        timestamp,
        seg_id
    FROM 
    # Have to query the pipe_vYYYYMMDD table since we're interested
    # in a non-fishing/carrier vessel. 
    `world-fishing-827.gfw_research.pipe_v20201001` 
    WHERE 
        DATE(_PARTITIONTIME) BETWEEN '2017-10-24' AND '2017-11-06'
        # only include this vessel
        AND ssvid = '352894000'   
        # only include good semgents
        AND seg_id IN (
        SELECT
            seg_id
        FROM 
            good_segments)
    ),

    #######################################################
    # Filter vessel activity to only include vessels of interest
    vessel_activity_filtered AS (
    SELECT *
    FROM vessel_activity
    JOIN seg_info
     USING(seg_id) # join using seg_id rather than ssvid
    )


#######################################################
# Return fishing data
SELECT *
FROM vessel_activity_filtered