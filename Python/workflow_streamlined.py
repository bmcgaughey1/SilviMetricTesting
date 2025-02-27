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
from assetCatalog import *

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
    # outlier filtering based on classification and height filtering after computing HAG in the pipeline used
    # to pump data into SilviMetric.
    #
    # Ground models for the area were derived from class 2 points in a FUSION run.

    ########## Setup #############
    project_name = "Plumas_VRT"
    file_pattern = "*.copc.laz"
    resolution = 30.0
    HAG_method = "vrt"                       # choices: "vrt", "delaunay", "nn"
    min_HAG = 2.0
    max_HAG = 150.0

    data_folder = "H:/FUSIONTestData"                               # COPC tiles from MPC, not normalized but have class 2 points
    ground_folder = "H:/FUSIONTestData/ground"
    ground_file_pattern = "*.img"

    ########## Paths ##########
    # get path to this python file. Outputs are in folders relative to this code.
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))     # folder containing this python file

    db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_{HAG_method}.tdb")
    db_dir = db_dir_path.as_posix()
    out_dir = (curpath / f"../TestOutput/{project_name}_{HAG_method}_tifs").as_posix()

    pipeline_filename = Path(curpath  / f"../TestOutput/__pl__.json")
    ground_VRT_filename = Path(curpath  / f"../TestOutput/__grnd__.vrt")
    
    ########## Collect and prepare assets: point tiles and DEM tiles ##########
    # get list of assets in data folder...could also be a list of URLs
    # assetCatalog only works for point files
    cat = assetCatalog(data_folder, file_pattern, testtype='pyproj')
    if not cat.is_complete():
        raise Exception(f"No point assets found in {data_folder} or assets are missing srs\n")

    # get list of ground files
    ground_assets = inventory_assets(ground_folder, ground_file_pattern)
    
    if len(ground_assets) == 0:
        raise Exception(f"No ground files found in {ground_folder}\n")

    # build ground VRT...resampleAlg isn't used because we don't do any warping
    gdal.UseExceptions()
    #opt = gdal.BuildVRTOptions(resampleAlg='bilinear')
    try:
        gdal.BuildVRT(ground_VRT_filename, ground_assets)
    except:
        raise Exception(f"Could not create VRT for DEM data: {ground_VRT_filename}")
    
    ######### create db #########
    # delete existing database, add metrics and create database
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db(bounds, resolution, srs, db_dir, 'pixelispoint')      # uses default set of metrics...broken as of 1/30/2025
    db_metric_subset(cat.overallbounds, resolution, cat.srs, db_dir, alignment = 'aligntocenter')

    # alignment
    # 'pixelispoint' = 'aligntocenter'
    # 'pixelisarea' = 'aligntocorner'

    # walk through assets, scan and shatter
    for asset in cat.assets:
        print(f"Processing asset: {asset.filename}\n")

        if HAG_method.lower() == "vrt":
            p = build_pipeline(asset.filename
                               , skip_classes = [7,9,18]        # skip points classified as outliers or water
                               , skip_overlap = False           # keep points flagged as overlap
                               , HAG_method = "vrt"             # use VRT for normalization
                               , ground_VRT = ground_VRT_filename
                               , min_HAG = min_HAG              # Minimum height for points used for metrics
                               , max_HAG = max_HAG              # maximum height for points used for metrics...this can help with unclassified outliers
                               , HAG_replaces_Z = True          # replace Z dimension with HAG
                               )
        if HAG_method.lower() == "delaunay":
            p = build_pipeline(asset.filename
                               , skip_classes = [7,9,18]        # skip points classified as outliers or water
                               , skip_overlap = False           # keep points flagged as overlap
                               , HAG_method = "delaunay"
                               , min_HAG = min_HAG              # Minimum height for points used for metrics
                               , max_HAG = max_HAG              # maximum height for points used for metrics...this can help with unclassified outliers
                               , HAG_replaces_Z = True          # replace Z dimension with HAG
                               )
        if HAG_method.lower() == "nn":
            p = build_pipeline(asset.filename
                               , skip_classes = [7,9,18]        # skip points classified as outliers or water
                               , skip_overlap = False           # keep points flagged as overlap
                               , HAG_method = "nn"
                               , min_HAG = min_HAG              # Minimum height for points used for metrics
                               , max_HAG = max_HAG              # maximum height for points used for metrics...this can help with unclassified outliers
                               , HAG_replaces_Z = True          # replace Z dimension with HAG
                               )

        # write pipeline file so we can pass it to scan and shatter
        # we write this in a separate step so we can add additional stages if needed
        write_pipeline(p, pipeline_filename)

        # scan...pass bounds for individual asset
        scan_info = sc(asset.bounds, pipeline_filename, db_dir)
        
        # use recommended tile size
        tile_size = int(scan_info['tile_info']['recommended'])
        #tile_size = int(scan_info['tile_info']['mean'])
        
        # shatter
        sh(asset.bounds, tile_size, pipeline_filename, db_dir)

        print(f"Finished asset: {asset.filename}\n")

    print(f"Finished all assets!!\n")

    # extract rasters...all metrics
    ex(db_dir, out_dir)
