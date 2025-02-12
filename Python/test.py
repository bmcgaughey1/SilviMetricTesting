import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree
from osgeo import gdal, ogr, osr
import pyproj
from osgeo.osr import SpatialReference

import planetary_computer as pc

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean

from smhelpers import build_pipeline, write_pipeline, scan_for_srs, scan_for_bounds, scan_asset_for_bounds, transform_bounds

""" 
# test working with list of planetary computer asset URLs
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
print(scan_asset_for_bounds(signedURLs[0]))

#print(scan_for_bounds(signedURLs, 30))

# building pipeline with signed URL seems to fail. It is almost like PDAL isn't recognizing that the file is COPC so it doesn't use the correct reader name when building the stage tag
# error message is something like tag name can't start with a number '1'...
 """

"""
# test pipeline writing
p = build_pipeline("test.laz", skip_classes = [7,18,9], override_srs = "EPSG:26911", out_srs = "EPSG:26910")
# write pipeline file
write_pipeline(p, "../TestOutput/__testpl__.json")
"""

""" 
# test for srs extraction
print(scan_for_srs(["H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"]))
print(scan_asset_for_bounds("H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"))
 """

""" 
# testing bounding box alignment
b = scan_asset_for_bounds("H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz")
print(f"file bounds: {b}")
b.adjust_to_cell_lines(30)
print(f"adjusted file bounds: {b}")
 """

""" 
# convert .laz files to COPC
folder = "H:/FUSIONTestData/normalized"
# get list of COPC assets in data folder...could also be a list of URLs
assets = [fn.as_posix() for fn in Path(folder).glob("*.laz")]

for asset in assets:
    # build pipeline
    print(asset)
    p = pdal.Reader(asset)
    p |= pdal.Writer.copc(asset.replace(".laz", ".copc.laz"))

    # execute
    p.execute()

 """

""" ogr.UseExceptions()
srs = scan_for_srs(["H:/FUSIONTestData/USGS_LPC_CA_NoCAL_Wildfires_PlumasNF_B2_2018_w2130n2145.copc.laz"])

crs = pyproj.CRS.from_json(srs)
crs.is_exact_same(crs2)
wkt = crs.to_wkt()
print(wkt)
 """

""" # test srs scanning and pyproj method
data_folder = "H:/FUSIONTestData"                               # COPC tiles from MPC, not normalized but have class 2 points
assets = [fn.as_posix() for fn in Path(data_folder).glob("*.copc.laz")]

if len(assets) == 0:
    raise Exception(f"No point assets found in {data_folder}\n")

# get srs for point tiles...also check that all assets have same sts
srs = scan_for_srs(assets, all_must_match = True, testtype='pyproj')

print(srs)
 """


# testing for URL
osr.UseExceptions()
base = 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/stac/'
assets = [
    'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
    'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz',
    'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
    'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
    ]
srs = scan_for_srs([assets[0]], all_must_match = True, testtype='pyproj')
#print(srs)
bnds = scan_for_bounds(assets)
print(bnds)

crs = pyproj.CRS.from_json(srs)
in_sr = osr.SpatialReference(crs.to_wkt())
print(in_sr.IsGeographic())
print(in_sr.GetAxisMappingStrategy())

out_srs =  osr.SpatialReference()
out_srs.ImportFromEPSG(26908)
print(out_srs.IsGeographic())
print(out_srs.GetAxisMappingStrategy())


out_srs =  osr.SpatialReference()
out_srs.ImportFromEPSG(26908)
tbnds = transform_bounds(bnds, srs, out_srs.ExportToPROJJSON())
print(tbnds)
