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

###############################################################################    
##########################  F U N C T I O N S  ################################
###############################################################################    
######## Create Metric ########
# Metrics give you the ability to define methods you'd like applied to the data
# Here we define, the name, the data type, and what values we derive from it.
def make_metric():
    def p75(arr: np.ndarray):
        return np.percentile(arr, 75)

    return Metric(name='p75', dtype=np.float32, method = p75)

###### Create Storage #####
# This will create a tiledb database, same as the `initialize` command would
# from the command line. Here we'll define the overarching bounds, which may
# extend beyond the current dataset, as well as the CRS of the data, the list
# of attributes that will be used, as well as metrics. The config will be stored
# in the database for future processes to use.
def db_metric_subset(normalized):
    perc_75 = make_metric()
    if normalized:
        attrs = [
            Pdal_Attributes[a]
            for a in ['Z', 'Intensity', 'HeightAboveGround']
        ]
    else:
        attrs = [
            Pdal_Attributes[a]
            for a in ['Z', 'Intensity']
        ]

    metrics = [ mean, sm_max, sm_min ]
    metrics.append(perc_75)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

def db(normalized):
    #perc_75 = make_metric()
    if normalized:
        attrs = [
            Pdal_Attributes[a]
            for a in ['Z', 'Intensity', 'HeightAboveGround']
        ]
    else:
        attrs = [
            Pdal_Attributes[a]
            for a in ['Z', 'Intensity']
        ]

    #metrics = [ mean, sm_max, sm_min ]
    #metrics.append(perc_75)
    # metrics = [grid_metrics]
    # st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        # attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, tdb_dir=db_dir)
    storage = Storage.create(st_config)

####### Perform Scan #######
# The Scan step will perform a search down the resolution tree of the COPC or
# EPT file you've supplied and will provide a best guess of how many cells per
# tile you should use for this dataset.
def sc(b, pf):
    return scan(tdb_dir=db_dir, pointcloud=pf, bounds=b)

###### Perform Shatter #####
# The shatter process will pull the config from the database that was previously
# made and will populate information like CRS, Resolution, Attributes, and what
# Metrics to perform from there. This will split the data into cells, perform
# the metric method over each cell, and then output that information to TileDB
def sh(b, tile_size, pf):
    sh_config = ShatterConfig(tdb_dir=db_dir, date=datetime.datetime.now(),
        filename=pf, tile_size=tile_size, bounds=b)
    shatter(sh_config)

###### Perform Extract #####
# The Extract step will pull data from the database for each metric/attribute combo
# and store it in an array, where it will be output to a raster with the name
# `m_{Attr}_{Metric}.tif`. By default, each computed metric will be written
# to the output directory, but you can limit this by defining which Metric names
# you would like
def ex():
    ex_config = ExtractConfig(tdb_dir=db_dir, out_dir=out_dir)
    extract(ex_config)

###############################################################################    
##########################       C O D E      #################################
###############################################################################    
# make sure script is being run directly and not imported into another script
if __name__ == "__main__":
    ########## Setup #############
    # Create a path for our current working directory, as well as the path
    # to our lidar data, the path to the database directory, and the path to the
    # directory that will house the raster data.
    curpath = Path(os.path.dirname(os.path.realpath(__file__)))

    # Use test data from Plumas National Forest In CA. These data have outliers classified as 7 & 18. These data
    # also have outliers marked as overlap points but not as outliers (bad outlier classification). In addition,
    # there are good points marked as overlap that should be kept. Easy solution is to drop all outliers but 
    # this will drop lots of good points. Best solution would be to add outlier filtering to pipeline or use
    # a height filter after computing HAG. Ground models were derived from class 2 points in a FUSION run.

    # flag when using data normalized by FUSION instead of doing normalization with PDAL
    normalized = True

    if normalized:
        # data normalized using FUSION
        folder = "H:/FUSIONTestData/normalized/COPC/subset"
    
        db_dir_path = Path(curpath  / "plumas_normalized.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / "plumas_normalized_tifs").as_posix()
    else:
        # original data...no HAG
        folder = "H:/FUSIONTestData"
    
        db_dir_path = Path(curpath  / "plumas.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / "plumas_tifs").as_posix()

    groundFolder = "H:/FUSIONTestData/ground"

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    resolution = 30 # 30 meter resolution

    # get list of COPC assets in data folder...could also be a list of URLs
    assets = [fn.as_posix() for fn in Path(folder).glob("*.copc.laz")]

    if len(assets) == 0:
        print(f"No point assets found in {folder}\n")
        quit()

    # get list of ground files
    groundFiles = [fn.as_posix() for fn in Path(groundFolder).glob("*.img")]
    
    if len(groundFiles) == 0:
        print(f"No ground files found in {groundFolder}\n")
        quit()

    # build ground VRT
    gdal.UseExceptions()
    gvrt = gdal.BuildVRT(ground_VRT_filename, groundFiles)
    
    # get overall bounding box for point data and adjust to cell lines
    bounds = scan_for_bounds(assets, resolution)
    #bounds = scan_asset_for_bounds(assets[0])

    # get srs...also check that all assets have same sts
    srs = scan_for_srs(assets, all_must_match = True)
       
    if srs == "":
        print(f"Missing or mismatched srs in assets\n")
        quit()
        
    # delete existing database, add metrics and create database
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db(normalized)
    db_metric_subset(normalized)

    # walk through assets
    for asset in assets:
    #asset = assets[0]
    #if True:
        # print(f"Processing asset: {asset}\n")
        if normalized:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, do_HAG = False, ground_VRT = ground_VRT_filename, min_HAG = 2.0)

            # add height filtering manually since Z is acutally HAG
            p |= pdal.Filter.expression(expression = f"Z >= 2.0 && Z <= 150.0")
        else:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = True, do_HAG = True, ground_VRT = ground_VRT_filename, min_HAG = 2.0)

        # write pipeline file so we can pass it to scan and shatter
        write_pipeline(p, pipeline_filename)

        # get bounds for individual asset...not sure if this is necessary...can you use the full extent of all data?
        fb = scan_asset_for_bounds(asset)
        
        # scan
        scan_info = sc(fb, pipeline_filename)
        
        # use recommended tile size
        tile_size = int(scan_info['tile_info']['recommended'])
        
        # shatter
        sh(fb, tile_size, pipeline_filename)

        # print(f"Finished asset: {asset}\n")

    print(f"Finished all assets!!\n")

    # extract rasters
    ex()
