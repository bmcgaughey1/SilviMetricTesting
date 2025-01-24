import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree
from osgeo import gdal

import planetary_computer as pc

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean

def build_pipeline(filename, add_classes = [], skip_classes = [], skip_synthetic = True, skip_keypoint = False, skip_withheld = True, skip_overlap = False):
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

def scan_for_bounds(files, resolution, adjust_to_cell_lines = True):
    # bogus bounds to start
    bounds = Bounds(sys.float_info.max, sys.float_info.max, sys.float_info.min, sys.float_info.min)

    # use PDAL python bindings to get bounds for each tile and update overall bounds
    for file in files:
        reader = pdal.Reader(file)
        p = reader.pipeline()
        print(p.pipeline)
        qi = p.quickinfo[reader.type]
        fb = Bounds.from_string((json.dumps(qi['bounds'])))

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

def scan_for_srs(files, all_must_match = True):
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
                return ""
                
    return srs

###### scan an individual file or URL for bounding box ######
# Use PDAl wuickinfo to get bounding box
#
# returns silvimetric.resources.bounds.Bounds object
def scan_file_for_bounds(file):
    reader = pdal.Reader(file)
    p = reader.pipeline()
    f = open("__test__.json", "w")
    f.write(p.pipeline)
    f.close()
    qi = p.quickinfo[reader.type]
    fb = Bounds.from_string((json.dumps(qi['bounds'])))
    
    return fb
    
# groundFolder = "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/BareGround_1METERS"

# # get list of ground files
# groundFiles = [fn.as_posix() for fn in Path(groundFolder).glob("*.img")]

# # build ground VRT
# gdal.UseExceptions()
# gvrt = gdal.BuildVRT("__grnd__.vrt", groundFiles)

# folder = "H:/FUSIONTestData"
# resolution = 30

# # get list of COPC files in data folder...could also be a list of URLs
# files = [fn.as_posix() for fn in Path(folder).glob("*.copc.laz")]

# print(scan_for_bounds(files, resolution = 30))

# print(scan_for_srs(files))

# # p = build_pipeline("test.laz", [0,1,2], skip_classes = [7,18,9])

# # # write pipeline file
# # f = open("__pl__.json", "w")
# # f.write(p.pipeline)
# # f.close()

# read list of planetary computer asset URLs
f = open("H:/FUSIONTestData/MPCTileURLs.txt", 'r')
URLs = f.readlines()
f.close()
#print(URLs)

signedURLs = [""] * len(URLs)
for i in range(0, len(URLs)):
    signedURLs[i] = pc.sign(URLs[i])
    
# testing planetary computer access
#URL = "https://usgslidareuwest.blob.core.windows.net/usgs-3dep-copc/usgs-copc/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018/copc/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2133n2147.copc.laz"
#signedURLs = pc.sign(URL)

print(signedURLs[0])
print(scan_file_for_bounds(signedURLs[0]))

#print(scan_for_bounds(signedURLs, 30))

# building pipeline with signed URL seems to fail. It is almost like PDAL isn't recognizing that the file is COPC so it doesn't use the correct reader name when building the stage tag
# error message is something like tag name can't start with a number '1'...