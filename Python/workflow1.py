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

###############################################################################    
##########################  F U N C T I O N S  ################################
###############################################################################    
###### scan for srs ######
# scan a list of files or URLs for srs info. Optionally check that all files
# in list have same srs.
#
# Returns valid srs if successful, empty string otherwise
def scan_for_srs(files, all_must_match = True):
    """Use PDAL quickinfo to get srs for first file in list of files. Optionally,
    check that all files in list have same srs.

    :raises Exception: srs for files in list are different from srs for first file
    :return: srs string
    """

    if len(files) == 0:
        return ""
        
    # use PDAL python bindings to find the srs of our data...look at first file
    reader = pdal.Reader(files[0])
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    srs = json.dumps(qi['srs']['json'])
    
    # check for matching SRS for all files...case insensitive
    if all_must_match:
        for i in range(1, len(files)):
            reader = pdal.Reader(files[i])
            p = reader.pipeline()
            qi = p.quickinfo[reader.type]
            fsrs = json.dumps(qi['srs']['json'])
            if fsrs.lower() != srs.lower():
                raise Exception(f"srs for file: {files[i]} ({fsrs}) dose not match srs for first file: {files[0]} ({srs})")
                
    return srs

###### scan an individual file or URL for bounding box ######
# Use PDAl wuickinfo to get bounding box
#
# returns silvimetric.resources.bounds.Bounds object
def scan_file_for_bounds(file):
    """Use PDAL quickinfo to get bounding box for data in file.

    :return: Return SilviMetric Bounds object
    """

    reader = pdal.Reader(file)
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    fb = Bounds.from_string((json.dumps(qi['bounds'])))
    
    return fb
    
###### scan for bounding box for a set of point files ######
# Use PDAL quickinfo to get bounding box for each file and build an overall bounding box.
# Optionally adjust (expand) bounding box to fall on cell boundaries.
#
# returns silvimetric.resources.bounds.Bounds object
def scan_for_bounds(files, resolution, adjust_to_cell_lines = True):
    """Use PDAL quickinfo to get overall bounding box for data in a list of files.

    :return: Return SilviMetric Bounds object optionally, adjusted to cell lines
    """

    # bogus bounds to start
    bounds = Bounds(sys.float_info.max, sys.float_info.max, sys.float_info.min, sys.float_info.min)

    # use PDAL python bindings to get bounds for each tile and update overall bounds
    for file in files:
        fb = scan_file_for_bounds(file)

        # compare bounds
        if fb.minx < bounds.minx:
            bounds.minx = fb.minx
        if fb.miny < bounds.miny:
            bounds.miny = fb.miny
        if fb.maxx > bounds.maxx:
            bounds.maxx = fb.maxx
        if fb.maxy > bounds.maxy:
            bounds.maxy = fb.maxy

    if adjust_to_cell_lines:
        bounds.adjust_to_cell_lines(resolution)
        
    return bounds

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
        # for a in ['Intensity', 'HeightAboveGround']
        for a in ['Z', 'Intensity', 'HeightAboveGround']
    ]
    metrics = [ mean, sm_max, sm_min ]
    metrics.append(perc_75)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

def db():
    #perc_75 = make_metric()
    attrs = [
        Pdal_Attributes[a]
        # for a in ['Intensity', 'HeightAboveGround']
        for a in ['Z', 'Intensity', 'HeightAboveGround']
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

###### Build pipeline used to get data ######
# This allows us to read individual files and shatter them. Pipeline
# does some simple filtering using classification values and flags.
# Pipeline also normalizes points using VRT and does simple HAG filtering.
# Filtering is done in two steps. First using classification values
# and flags, then, after normalization, using HAG. This reduces number
# of points being normalized.
def build_pipeline(pointdata_filename
                    , add_classes = []
                    , skip_classes = []
                    , skip_synthetic = True
                    , skip_keypoint = False
                    , skip_withheld = True
                    , skip_overlap = False
                    , override_srs = ""
                    , do_HAG = False
                    , ground_VRT = ""
                    , min_HAG = -5.0
                    , max_HAG = 150.0
                    , out_srs = ""
                   ):
    """Create pipeline to feed points to SilveMetric

    :raises Exception: The same classes are included in add_classes and skip_classes
    :return: Return PDAL pipline
    """

    # check for classes that are in both add_classes and skip_classes
    if len(add_classes) and len(skip_classes):
        for cls in add_classes:
            if cls in skip_classes:
                raise Exception(f"You can't specify the same class in add_classes {add_classes} and skip_classes {skip_classes}")
                
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
        
    # build point reader stage
    stage = pdal.Reader(pointdata_filename)

    # override srs for points...
    if override_srs != "":
        stage._options['override_srs'] = f"{override_srs}"

    # build pipeline
    p = pdal.Pipeline([stage])

    if len(exp) > 0:
        p |= pdal.Filter.expression(expression = exp)
    
    # assumes DEM VRT and point data use same srs
    if do_HAG:
        p |= pdal.Filter.hag_dem(raster = ground_VRT, zero_ground = False)
        p |= pdal.Filter.expression(expression = f"HeightAboveGround >= {min_HAG} && HeightAboveGround <= {max_HAG}")
    
    # do projection after HAG so we can use source DEM VRT
    if out_srs != "":
        p |= pdal.Filter.reprojection(out_srs = f"{out_srs}")

    # return pipeline
    return p

###### write pipeline file ######
# Write pipeline to json file
def write_pipeline(p, filename):
    """Write pipeline

    :return: None
    """

    f = open(filename, "w")
    f.write(p.pipeline)
    f.close()

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
    folder = "H:/FUSIONTestData"
    groundFolder = "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/BareGround_1METERS"
    
    db_dir_path = Path(curpath  / "plumas.tdb")
    db_dir = db_dir_path.as_posix()
    out_dir = (curpath / "plumas_tifs").as_posix()

    pipeline_filename = "../TestOutput/__pl__.json"
    ground_VRT_filename = "../TestOutput/__grnd__.vrt"
    
    resolution = 30 # 30 meter resolution

    # get list of COPC files in data folder...could also be a list of URLs
    files = [fn.as_posix() for fn in Path(folder).glob("*.copc.laz")]

    if len(files) == 0:
        print(f"No point files found in {folder}\n")
        quit()

    # get list of ground files
    groundFiles = [fn.as_posix() for fn in Path(groundFolder).glob("*.img")]
    
    if len(groundFiles) == 0:
        print(f"No ground files found in {groundFolder}\n")
        quit()

    # build ground VRT
    gdal.UseExceptions()
    gvrt = gdal.BuildVRT(ground_VRT_filename, groundFiles)
    
    # get overall bounding box for point data
    bounds = scan_for_bounds(files, resolution)

    # adjust bounds to cell center origin to match FUSION outputs
    ##### This doesn't work...when storage is created, bounds are aligned to cells again
    # bounds.minx = bounds.minx - resolution / 2.0
    # bounds.miny = bounds.miny - resolution / 2.0
    # bounds.maxx = bounds.maxx + resolution / 2.0
    # bounds.maxy = bounds.maxy + resolution / 2.0
    
    # get srs...also check that all files have same sts
    srs = scan_for_srs(files, all_must_match = True)
       
    if srs == "":
        print(f"Missing or mismatched srs in files\n")
        quit()
        
    # delete existing database, add metrics and create database
    rmtree(db_dir, ignore_errors=True)
    make_metric()
    # db()
    db_metric_subset()

    # walk through files
    for file in files:
        # print(f"Processing file: {file}\n")
        p = build_pipeline(file, skip_classes = [7,9,18], skip_overlap = True, do_HAG = True, ground_VRT = ground_VRT_filename, min_HAG = 2.0)

        # write pipeline file so we can pass it to scan and shatter
        write_pipeline(p, pipeline_filename)

        # get bounds for individual file...not sure if this is necessary...can you use the full extent of all data?
        fb = scan_file_for_bounds(file)
        
        # scan
        scan_info = sc(fb, pipeline_filename)
        
        # use recommended tile size
        tile_size = int(scan_info['tile_info']['recommended'])
        
        # shatter
        sh(fb, tile_size, pipeline_filename)
        # print(f"Finished file: {file}\n")

    print(f"Finished all files!!\n")

    # extract rasters
    ex()
