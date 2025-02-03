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
def db_metric_subset(bounds, resolution, srs, db_dir):
    perc_75 = make_metric()
    attrs = [
        Pdal_Attributes[a]
        for a in ['Z']
        #for a in ['Z', 'Intensity']
    ]

    #metrics = [ mean, sm_max, sm_min ]
    metrics = [ sm_max ]
    #metrics.append(perc_75)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

def db_metric_CHM(bounds, resolution, srs, db_dir):
    attrs = [
        Pdal_Attributes[a]
        for a in ['Z']
    ]

    metrics = [ sm_max ]
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

def db(bounds, resolution, srs, db_dir):
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
def sc(b, pf, db_dir):
    return scan(tdb_dir=db_dir, pointcloud=pf, bounds=b)

###### Perform Shatter #####
# The shatter process will pull the config from the database that was previously
# made and will populate information like CRS, Resolution, Attributes, and what
# Metrics to perform from there. This will split the data into cells, perform
# the metric method over each cell, and then output that information to TileDB
def sh(b, tile_size, pf, db_dir):
    sh_config = ShatterConfig(tdb_dir=db_dir, date=datetime.datetime.now(),
        filename=pf, tile_size=tile_size, bounds=b)
    shatter(sh_config)

###### Perform Extract #####
# The Extract step will pull data from the database for each metric/attribute combo
# and store it in an array, where it will be output to a raster with the name
# `m_{Attr}_{Metric}.tif`. By default, each computed metric will be written
# to the output directory, but you can limit this by defining which Metric names
# you would like
def ex(db_dir, out_dir):
    ex_config = ExtractConfig(tdb_dir=db_dir, out_dir=out_dir)
    extract(ex_config)
