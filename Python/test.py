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

###### check for command line argument to run specific test ######
def testnum() -> int:
    # check for command line args
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    
    return 0

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

if testnum() == 1 or testnum() == 0:
    # testing for URL
    osr.UseExceptions()
    base = 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/stac/'
    assets = [
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz'
        #'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
        #'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
        ]
    srs = scan_for_srs(assets, all_must_match = True, testtype='pyproj')
    #print(srs)
    bnds = scan_for_bounds(assets)
    #print(bnds)

    crs = pyproj.CRS.from_json(srs)
    in_sr = osr.SpatialReference(crs.to_wkt())
    print(in_sr)
    print(in_sr.GetAxisName('GEOGCS', 0))
    print(in_sr.GetAxisName('GEOGCS', 1))
    #in_sr = osr.SpatialReference(crs.to_wkt())
    #print(in_sr.IsGeographic())
    #print(in_sr.GetAxisMappingStrategy())

    out_srs =  osr.SpatialReference()
    out_srs.ImportFromEPSG(26908)
    print(out_srs)
    print(out_srs.GetAxisName('PROJCS', 0))
    print(out_srs.GetAxisName('PROJCS', 1))
    #print(out_srs.IsGeographic())
    #print(out_srs.GetAxisMappingStrategy())


    out_srs =  osr.SpatialReference()
    out_srs.ImportFromEPSG(26908)
    osrs = out_srs.ExportToPROJJSON()
    tbnds = transform_bounds(bnds, srs, osrs)
    print(tbnds)
    print(tbnds.to_json())

if testnum() == 2 or testnum() == 0:
    osr.UseExceptions()
    base = 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/stac/'
    assets = [
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
        ]
    srs = scan_for_srs(assets, all_must_match = True, testtype='pyproj')
    print(f"JSON: {srs}\n")

    ppcrs = pyproj.CRS.from_json(srs)
    print(f"WKT: {ppcrs.to_wkt(output_axis_rule=False)}\n")
    print(f"WKT(axis): {ppcrs.to_wkt(output_axis_rule=True)}\n")

    ppcrs = pyproj.CRS.from_epsg(26908)
    print(f"WKT: {ppcrs.to_wkt(output_axis_rule=False)}\n")
    print(f"WKT(axis): {ppcrs.to_wkt(output_axis_rule=True)}\n")

if testnum() == 3 or testnum() == 0:
    assets = [
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz'
        #'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
        #'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
        ]
    srs = scan_for_srs(assets, all_must_match = True, testtype='pyproj')
    #print(srs)
    bnds = scan_for_bounds(assets)

    # proper JSON formatting for Bounds object with CRS
    bb = json.dumps({
            "minx": bnds.minx, 
            "miny": bnds.miny, 
            "maxx": bnds.maxx, 
            "maxy": bnds.maxy,
            "crs": pyproj.CRS.from_json(srs).to_wkt()
        }, 
        indent = 4,
        sort_keys = False)
    print(bb)

    file = open("../TestOutput/bounds.json", "w")

    # Write data to the file
    file.write(bb)

    # Close the file
    file.close()

