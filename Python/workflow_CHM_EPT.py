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
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig, ApplicationConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean
# from silvimetric.resources.metrics.__init__ import grid_metrics

from smhelpers import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_asset_for_bounds, inventory_assets
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
    project_name = "EPT_CHM"
    resolution = 1.5
    HAG_method = "delaunay"                       # choices: "delaunay", "nn"
    min_HAG = -100.0
    max_HAG = 150.0

    ########## Paths ##########
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))     # folder containing this python file

    # name for pipeline file that will be created to feed point data to SM
    pipeline_filename = "../TestOutput/__pl__.json"
    
    db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_{HAG_method}.tdb")
    db_dir = db_dir_path.as_posix()
    out_dir = (curpath / f"../TestOutput/{project_name}_{HAG_method}_tifs").as_posix()

    ########## Collect and prepare assets: EPT endpoint ##########
    assets = ["https://s3-us-west-2.amazonaws.com/usgs-lidar-public/WI_Oshkosh_3Rivers_FondDuLac_TL_2018/ept.json"]

    # get srs for point tiles...also check that all assets have same srs
    # get srs for point tiles...also check that all assets have same srs
    # Throws exception if first asset doesn't have srs or srs for assets don't match
    try:
        srs = scan_for_srs(assets)
    except:
        raise Exception("Problem with srs in assets")
       
    ######### create db #########
    # get overall bounding box for point data
    bounds = scan_for_bounds(assets, resolution)

    # delete existing database, add metrics and create database
    # db_metric_CHM() only includes the Z dimension (HAG in this case) and maximum value metric
    rmtree(db_dir, ignore_errors=True)
    db_metric_CHM(bounds, resolution, srs, db_dir, alignment = 'pixelisarea')

    # 'pixelispoint' = 'aligntocenter' FUSION metric alignment
    # 'pixelisarea' = 'aligntocorner'  FUSION CHM alignment

    ########## walk through assets, scan and shatter ##########
    for asset in assets:
        # print(f"Processing asset: {asset}\n")

        # build pipeline to feed data to SM
        if HAG_method.lower() == "delaunay":
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "delaunay", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True)
        if HAG_method.lower() == "nn":
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "nn", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True)

        # set HAG for points below ground to 0.0...FUSION sets points with negative height to 0.0 for CHM creation
        p |= pdal.Filter.assign(value = "Z = 0.0 WHERE Z < 0.0")

        # write pipeline file so we can pass it to scan and shatter
        write_pipeline(p, pipeline_filename)

        # get bounds for individual asset...not sure if this is necessary...can you use the full extent of all data?
        fb = scan_asset_for_bounds(asset)
        
        # scan
        scan_info = sc(fb, pipeline_filename, db_dir)
        
        # use recommended tile size
        tile_size = int(scan_info['tile_info']['recommended'])
        #tile_size = int(scan_info['tile_info']['mean'])
        
        # shatter
        sh(fb, tile_size, pipeline_filename, db_dir)

        # print(f"Finished asset: {asset}\n")

    print(f"Finished all assets!!\n")

    # extract rasters
    ex(db_dir, out_dir)
