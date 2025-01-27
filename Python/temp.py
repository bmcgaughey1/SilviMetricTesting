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

from workflow1 import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_file_for_bounds

""" 
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
 """

"""
p = build_pipeline("test.laz", skip_classes = [7,18,9], override_srs = "EPSG:26911", out_srs = "EPSG:26910")
# write pipeline file
write_pipeline(p, "__testpl__.json")
"""

""" 
# test for srs extraction
print(scan_for_srs(["H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"]))
print(scan_file_for_bounds("H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"))
 """