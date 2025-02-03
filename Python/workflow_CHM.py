import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree
from osgeo import gdal

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean
# from silvimetric.resources.metrics.__init__ import grid_metrics

from smhelpers import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_asset_for_bounds
from smfunc import make_metric, db_metric_subset, db_metric_CHM,  db, sc, sh, ex

###############################################################################    
##########################       C O D E      #################################
###############################################################################    
#
# This scenario creates a CHM using point data. SilviMetric was not really 
# designed for this task (single metric computed at high resolution) but
# people have been asking if it can be used to create a CHM. For this task,
# we only need the maximum HAG value for each cell so we need to limit the 
# metrics and dimensions. In addition, the CHM requires all points (no
# filtering using a height threshold).
#
# make sure script is being run directly and not imported into another script
# this isn't really needed since we have no functions in this module.
if __name__ == "__main__":
    # Test data is from USGS collection covering the Plumas National Forest In CA. These data have outliers 
    # classified as 7 & 18 and outliers marked as overlap points but not as outliers (bad outlier classification). 
    # In addition, there are good points marked as overlap that were used in the FUSION processing. 
    # 
    # One scenario is to drop all overlap points but this will drop lots of good points and outputs won't match
    # FUSION outputs. FUSION filters points by classification and height so the best solution would be to use
    # outlier filtering based on classification and height filtering after computing HAG to the pipeline used
    # to pump data into SilviMetric.
    #
    # Ground models for the area were derived from class 2 points in a FUSION run.

    ########## Setup #############
    project_name = "Plumas_CHM"
    resolution = 1.5
    use_normalized_point_data = False        # True: data already has HAG computed by FUSION, False: data has elevation

    ########## Paths ##########
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))     # folder containing this python file

    ground_folder = "H:/FUSIONTestData/ground"

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    # I have normalized point data using FUSION to test DEM interpolation methods and allow a
    # more consistent comparison with FUSION-derived outputs.
    if use_normalized_point_data:
        data_folder = "H:/FUSIONTestData/normalized/COPC"               # data normalized using FUSION
        #data_folder = "H:/FUSIONTestData/normalized/COPC/subset"        # subset of data normalized using FUSION

        db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_normalized.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / f"../TestOutput/{project_name}_normalized_tifs").as_posix()
    else:
        data_folder = "H:/FUSIONTestData"                               # COPC tiles from MPC, not normalized but have class 2 points
        db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_vrt.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / f"../TestOutput/{project_name}_vrt_tifs").as_posix()

    ########## Collect and prepare assets: point tiles and DEM tiles ##########
    # get list of COPC assets in data folder...could also be a list of URLs
    assets = [fn.as_posix() for fn in Path(data_folder).glob("*.copc.laz")]

    if len(assets) == 0:
        raise Exception(f"No point assets found in {data_folder}\n")

    # get srs for point tiles...also check that all assets have same sts
    srs = scan_for_srs(assets, all_must_match = True)
       
    if srs == "":
        print(f"Missing or mismatched srs in assets\n")
        quit()
        
    # get list of ground files
    ground_assets = [fn.as_posix() for fn in Path(ground_folder).glob("*.img")]
    
    if len(ground_assets) == 0:
        raise Exception(f"No ground files found in {ground_folder}\n")

    # build ground VRT
    gdal.UseExceptions()
    gvrt = gdal.BuildVRT(ground_VRT_filename, ground_assets)
    
    ######### create db #########
    # get overall bounding box for point data and adjust to cell lines
    bounds = scan_for_bounds(assets, resolution)

    # delete existing database, add metrics and create database
    # db_metric_CHM() only includes the Z dimension (HAG in this case) and maximum value metric
    rmtree(db_dir, ignore_errors=True)
    db_metric_CHM(bounds, resolution, srs, db_dir)

    ########## walk through assets, scan and shatter ##########
    for asset in assets:
        # print(f"Processing asset: {asset}\n")

        # build pipeline to feed data to SM
        if use_normalized_point_data:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False)

            # add height filtering manually since Z in data is actually HAG. build_pipeline() does filtering on HeightAboveGround, then ferries HeightAboveGround as Z
            p |= pdal.Filter.expression(expression = f"Z >= 0.0 && Z <= 150.0")
        else:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "vrt", ground_VRT = ground_VRT_filename, min_HAG = 0.0, HAG_replaces_Z = True)
            #p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "delaunay", ground_VRT = ground_VRT_filename, min_HAG = 0.0, HAG_replaces_Z = True)
            #p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "nn", ground_VRT = ground_VRT_filename, min_HAG = 0.0, HAG_replaces_Z = True)

        # write pipeline file so we can pass it to scan and shatter
        write_pipeline(p, pipeline_filename)

        # get bounds for individual asset...not sure if this is necessary...can you use the full extent of all data?
        fb = scan_asset_for_bounds(asset)
        
        # scan
        scan_info = sc(fb, pipeline_filename, db_dir)
        
        # use recommended tile size
        #tile_size = int(scan_info['tile_info']['recommended'])
        tile_size = int(scan_info['tile_info']['mean'])
        
        # shatter
        sh(fb, tile_size, pipeline_filename, db_dir)

        # print(f"Finished asset: {asset}\n")

    print(f"Finished all assets!!\n")

    # extract rasters
    ex(db_dir, out_dir)
