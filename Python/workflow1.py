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
def db_metric_subset():
    perc_75 = make_metric()
    attrs = [
        Pdal_Attributes[a]
        for a in ['Z', 'Intensity']
    ]

    metrics = [ mean, sm_max, sm_min ]
    metrics.append(perc_75)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

def db():
    # use full set of gridmetrics...not working as of 1/30/2025
    #perc_75 = make_metric()
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
    resolution = 30
    use_normalized_point_data = False        # True: data already has HAG, False: data has elevation

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
        db_dir_path = Path(curpath  / f"../TestOutput/{project_name}_nn.tdb")
        db_dir = db_dir_path.as_posix()
        out_dir = (curpath / f"../TestOutput/{project_name}_nn_tifs").as_posix()

    ground_folder = "H:/FUSIONTestData/ground"

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    resolution = 30 # 30 meter resolution

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
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db()      # uses default set of metrics...broken as of 1/30/2025
    db_metric_subset()

    # walk through assets, scan and shatter
    for asset in assets:
        # print(f"Processing asset: {asset}\n")
        if use_normalized_point_data:
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = False, HAG_method = None, ground_VRT = ground_VRT_filename, min_HAG = 2.0)

            # add height filtering manually since Z in data is acutally HAG
            p |= pdal.Filter.expression(expression = f"Z >= 2.0 && Z <= 150.0")
        else:
            #p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = True, HAG_method = "dem", ground_VRT = ground_VRT_filename, min_HAG = 2.0, HAG_replaces_Z = True)
            p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = True, HAG_method = "nn", ground_VRT = ground_VRT_filename, min_HAG = 2.0, HAG_replaces_Z = True)
            #p = build_pipeline(asset, skip_classes = [7,9,18], skip_overlap = True, HAG_method = "nn", ground_VRT = ground_VRT_filename, min_HAG = 2.0, HAG_replaces_Z = True)

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
