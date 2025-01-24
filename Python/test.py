import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean



# from osgeo import gdal

# input_files = ["file1.tif", "file2.tif", "file3.tif"]
# output_vrt = "output.vrt"

# gdal.BuildVRT(output_vrt, input_files)


########## Setup #############

# Here we create a path for our current working directory, as well as the path
# to our forest data, the path to the database directory, and the path to the
# directory that will house the raster data.
curpath = Path(os.path.dirname(os.path.realpath(__file__)))
# filename = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/USGS_LPC_IN_ET_B7_Union_2012_LAS_2016/ept.json"
# filename = "../autzen-classified.copc.laz"
# filename = "H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"

# Use test data from Plumas National Forest In CA. These data have outliers classified as 7 & 18. These data
# also have outliers marked as overlap points but not as outliers (bad outlier classification). In addition,
# there are good points marked as overlap that shoujld be kept. Easy solution is to drop all outliers but 
# this will drop lots of good points. Best solution would be to add outlier filtering to pipeline or use
# a height filter after computing HAG.
folder = "H:/FUSIONTestData"

bounds = Bounds(sys.float_info.max, sys.float_info.max, sys.float_info.min, sys.float_info.min)

# get list of COPC files in folder
files = [fn.as_posix() for fn in Path(folder).glob("*.copc.laz")]

# pipeline = pdal.Reader("1.2-with-color.las") | pdal.Filter.sort(dimension="X")

# testing
# p = pdal.Reader(files[0]) | pdal.Filter.expression(expression = '((Classification != 0) && (Classification != 7) && (Classification != 18))')
# print(p.quickinfo)
# print(p.pipeline)

# f = open("__pl__.json", "w")
# f.write(p.pipeline)
# f.close()

# quit()

# use PDAL python bindings to get bounds for each tile and merge into a single, overall bounds
#for file in files:
file = files[10]
if True:
    # print(file)
    reader = pdal.Reader(file)
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    fb = Bounds.from_string((json.dumps(qi['bounds'])))
    # print(f"Tile bounds: {fb}\n")
    # compare bounds
    if fb.minx < bounds.minx:
        bounds.minx = fb.minx
    if fb.miny < bounds.miny:
        bounds.miny = fb.miny
    if fb.maxx > bounds.maxx:
        bounds.maxx = fb.maxx
    if fb.maxy > bounds.maxy:
        bounds.maxy = fb.maxy

# print(f"Overall bounds: {bounds}\n")

#quit()

db_dir_path = Path(curpath  / "plumas.tdb")

db_dir = db_dir_path.as_posix()
out_dir = (curpath / "plumas_tifs").as_posix()
resolution = 30 # 30 meter resolution

# adjust bounds
bounds.adjust_to_cell_lines(resolution)

# use PDAL python bindings to find the srs of our data...only need to look at first file
reader = pdal.Reader(files[0])
p = reader.pipeline()
qi = p.quickinfo[reader.type]
# bounds = Bounds.from_string((json.dumps(qi['bounds'])))
srs = json.dumps(qi['srs']['json'])

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

def db():
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

def build_pipeline(filename, add_classes, skip_classes, skip_synthetic = True, skip_keypoint = False, skip_withheld = True, skip_overlap = False):
    # build expression
    exp = ""
    
    # classes to keep
    if len(add_classes):
        exp = exp + "("
        for cls in add_classes:
            exp = exp + f"(Classification == {cls})"
            if cls != add_classes[len(add_classes) - 1]:
                exp = exp + " || "
        exp = exp + ")"

    # classes to drop
    if len(skip_classes):
        if len(exp) > 0:
            exp = exp + " && ("
        else:
            exp = exp + "("
            
        for cls in skip_classes:
            exp = exp + f"(Classification != {cls})"
            if cls != skip_classes[len(skip_classes) - 1]:
                exp = exp + " && "
        exp = exp + ")"
        
    # synthetic
    if skip_synthetic:
        if len(exp) > 0:
            exp = exp + " && (Synthetic != 1)"
        else:
            exp = exp + "(Synthetic != 1)"
            
    if skip_keypoint:
        if len(exp) > 0:
            exp = exp + " && (Keypoint != 1)"
        else:
            exp = exp + "(Keypoint != 1)"

    if skip_withheld:
        if len(exp) > 0:
            exp = exp + " && (Withheld != 1)"
        else:
            exp = exp + "(Withheld != 1)"

    if skip_overlap:
        if len(exp) > 0:
            exp = exp + " && (Overlap != 1)"
        else:
            exp = exp + "(Overlap != 1)"
    
    # wrap final expression
    if len(exp) > 0:
        exp = "(" + exp + ")"
        
    # build pipeline
    p = pdal.Reader(filename)

    if len(exp) > 0:
        p = p | pdal.Filter.expression(expression = exp)
    
    # return pipeline
    return p

    
# make sure script is being run directly and not imported into another script
if __name__ == "__main__":
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    db()

# pipeline = pdal.Reader("1.2-with-color.las") | pdal.Filter.sort(dimension="X")
    # walk through files
 #   for file in files:
    if True:
        # print(f"Processing file: {file}\n")
        p = pdal.Reader(file) | pdal.Filter.expression(expression = '((Overlap != 1) && (Classification != 7) && (Classification != 18))')

        # write pipeline file
        f = open("__pl__.json", "w")
        f.write(p.pipeline)
        f.close()
        
        reader = pdal.Reader(file)
        p = reader.pipeline()
        qi = p.quickinfo[reader.type]
        fb = Bounds.from_string((json.dumps(qi['bounds'])))
        scan_info = sc(fb, "__pl__.json")
        # grab the tile size for this chunk so we can operate at an apropriate rate
        # the variance in tile density leads to a large standard deviation,
        # so we'll just use the mean for this scenario
        tile_size = int(scan_info['tile_info']['mean'])
        sh(fb, tile_size, "__pl__.json")
        # print(f"Finished file: {file}\n")

    print(f"Finished all files!!\n")

    # this is a large dataset, and silvimetric supports incremental insertion
    # so we'll be splitting up our bounds and shattering them that way.
    # for b2 in bounds.bisect():
        # for b1 in b2.bisect():
            # print(f"Processing bounds: {b1}")
            # scan_info = sc(b1)
            # # grab the tile size for this chunk so we can operate at an apropriate rate
            # # the variance in tile density leads to a large standard deviation,
            # # so we'll just use the mean for this scenario
            # tile_size = int(scan_info['tile_info']['mean'])
            # sh(b1, tile_size)
            # print(f"Finished bounds: {b1}\n")

    ex()
