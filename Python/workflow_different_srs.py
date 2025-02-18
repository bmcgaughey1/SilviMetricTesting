import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
import pyproj
from shutil import rmtree
from osgeo import gdal, osr

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean
# from silvimetric.resources.metrics.__init__ import grid_metrics

from smhelpers import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_asset_for_bounds, transform_bounds
from smfunc import make_metric, db_metric_subset, db, sc, sh, ex

###############################################################################    
##########################       C O D E      #################################
###############################################################################    
# make sure script is being run directly and not imported into another script
# this isn't really needed since we have no functions in this module.
if __name__ == "__main__":
    # Test data is from NOAA COPC collection covering part of Wrangell Island, AK. These data have outliers 
    # classified as 7 & 18. There are some misclassified outliers that are trees but these are hard to "fix".
    # These data were flown with a Reigl VUX scanner from low altitude and "wandering" flight lines. Data are
    # good but not like a ty[ical aerial campaign.
    # 
    # Original data were transformed to EPSG:6318 but axes are swapped in LAZ files. I reprojected these into
    # EPSG:26907 using PDAL. We want metrics in EPSG:26908 so our ingest pipeline needs to use a bounding box 
    # in EPSG:26907, then reproject points to EPSG:26908 to compute metrics.
    #
    # As of 2/18/2025, this requires the boundsCRS branch of SilviMetric to add CRS to the bounding box when 
    # reading points.
    #

    ########## Setup #############
    project_name = "NOAA_S3"
    resolution = 30.0
    HAG_method = "delaunay"                       # choices: "vrt", "delaunay", "nn"
    min_HAG = 2.0
    max_HAG = 100.0

    ########## Paths ##########
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))     # folder containing this python file

    data_folder = "H:/NOAATestData/UTM7"                               # COPC tiles from MPC, not normalized but have class 2 points
    db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_{HAG_method}.tdb")
    db_dir = db_dir_path.as_posix()
    out_dir = (curpath / f"../TestOutput/{project_name}_{HAG_method}_tifs").as_posix()

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    ########## Collect and prepare assets: point tiles and DEM tiles ##########
    # get list of COPC assets in data folder...could also be a list of URLs
    assets = [fn.as_posix() for fn in Path(data_folder).glob("*.copc.laz")]

    if len(assets) == 0:
        raise Exception(f"No point assets found in {data_folder}\n")

    # list of COPC assets from NOAA...lat-lon
    # assets = [
    # 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
    # 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz',
    # 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
    # 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
    # ]

    # get srs for point tiles...also check that all assets have same srs
    # Throws exception if first asset doesn't have srs or srs for assets don't match
    try:
        srs = scan_for_srs(assets, all_must_match = True)
    except:
        raise Exception("Problem with srs in assets")

    # get overall bounding box for point data
    tbounds = scan_for_bounds(assets, resolution)

    # the bound is in EPSG:6318 lon-lat NAD83(2011) but we want our metrics in UTM zone 8
    osr.UseExceptions()

    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(26908)
    out_srs = out_srs.ExportToPROJJSON()

    # transform the bounding box...necessary because we want outputs to be in different srs than input point data
    bounds = transform_bounds(tbounds, srs, out_srs)

    ######### create db #########
    # delete existing database, add metrics and create database
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db(bounds, resolution, out_srs, db_dir, 'pixelispoint')      # uses default set of metrics...broken as of 1/30/2025
    db_metric_subset(bounds, resolution, out_srs, db_dir, alignment = 'pixelispoint')

    # walk through assets, scan and shatter
    for asset in assets:
        print(f"Processing asset: {asset}\n")
        if HAG_method.lower() == "delaunay":
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "delaunay", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True, out_srs='EPSG:26908')
        if HAG_method.lower() == "nn":
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = "nn", min_HAG = min_HAG, max_HAG = max_HAG, HAG_replaces_Z = True, out_srs='EPSG:26908')

        # write pipeline file so we can pass it to scan and shatter
        write_pipeline(p, pipeline_filename)

        # get bounds for individual asset
        tfb = scan_asset_for_bounds(asset)

        # transform the bounding box...necessary because we want outputs to be in different srs than input point data
        fb = transform_bounds(tfb, srs, out_srs)

        # scan
        scan_info = sc(fb, pipeline_filename, db_dir)
        #print(scan_info)

        # set tile size
        tile_size = int(scan_info['tile_info']['recommended'])
        #tile_size = int(scan_info['tile_info']['mean'])
        #tile_size = 5
        
        # shatter
        sh(fb, tile_size, pipeline_filename, db_dir)

        # print(f"Finished asset: {asset}\n")

    print(f"Finished all assets!!\n")

    # extract rasters
    ex(db_dir, out_dir)
