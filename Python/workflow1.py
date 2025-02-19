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

from smhelpers import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_asset_for_bounds, inventory_assets
from smfunc import make_metric, db_metric_subset, db, sc, sh, ex

###############################################################################    
##########################       C O D E      #################################
###############################################################################    
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
    project_name = "Plumas"
    resolution = 30.0
    use_normalized_point_data = False        # True: data already has HAG, False: data has elevation
    HAG_method = "vrt"                       # choices: "vrt", "delaunay", "nn"
    min_HAG = 2.0
    max_HAG = 150.0

    ########## Paths ##########
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))     # folder containing this python file

    if use_normalized_point_data:
        data_folder = "H:/FUSIONTestData/normalized/COPC"               # data normalized using FUSION
        #data_folder = "H:/FUSIONTestData/normalized/COPC/subset"        # subset of data normalized using FUSION

        db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_normalized.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / f"../TestOutput/{project_name}_normalized_tifs").as_posix()
    else:
        data_folder = "H:/FUSIONTestData"                               # COPC tiles from MPC, not normalized but have class 2 points
        db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_{HAG_method}.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / f"../TestOutput/{project_name}_{HAG_method}_tifs").as_posix()

    ground_folder = "H:/FUSIONTestData/ground"

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    ########## Collect and prepare assets: point tiles and DEM tiles ##########
    # get list of COPC assets in data folder...could also be a list of URLs
    #assets = [fn.as_posix() for fn in Path(data_folder).glob("*.copc.laz")]
    assets = inventory_assets(data_folder, "*.copc.laz")

    if len(assets) == 0:
        raise Exception(f"No point assets found in {data_folder}\n")

    # get srs for point tiles...also check that all assets have same sts
    srs = scan_for_srs(assets, all_must_match = True)
       
    if srs == "":
        print(f"Missing or mismatched srs in assets\n")
        quit()
        
    # get list of ground files
    #ground_assets = [fn.as_posix() for fn in Path(ground_folder).glob("*.img")]
    ground_assets = inventory_assets(ground_folder, "*.img")
    
    if len(ground_assets) == 0:
        raise Exception(f"No ground files found in {ground_folder}\n")

    # build ground VRT
    gdal.UseExceptions()
    gvrt = gdal.BuildVRT(ground_VRT_filename, ground_assets)
    
    ######### create db #########
    # get overall bounding box for point data and adjust to cell lines
    bounds = scan_for_bounds(assets, resolution)

    # delete existing database, add metrics and create database
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db(bounds, resolution, srs, db_dir, 'pixelispoint')      # uses default set of metrics...broken as of 1/30/2025
    db_metric_subset(bounds, resolution, srs, db_dir, alignment = 'pixelispoint')

    # walk through assets, scan and shatter
    for asset in assets:
        # print(f"Processing asset: {asset}\n")
        if use_normalized_point_data:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = None, ground_VRT = ground_VRT_filename)

            # add height filtering manually since Z in data is acutally HAG
            p |= pdal.Filter.expression(expression = f"Z >= {min_HAG} && Z <= {max_HAG}")
        else:
            if HAG_method.lower() == "vrt":
                p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "vrt", ground_VRT = ground_VRT_filename, min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True)
            if HAG_method.lower() == "delaunay":
                p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "delaunay", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True)
            if HAG_method.lower() == "nn":
                p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "nn", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True)

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
