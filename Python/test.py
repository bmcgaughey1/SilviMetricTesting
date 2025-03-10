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
import pandas
from osgeo.osr import SpatialReference

import planetary_computer as pc

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean

from assetCatalog import assetCatalog

from smhelpers import *

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

    ppsrs = pyproj.CRS.from_json(srs)
    print(f"WKT: {ppsrs.to_wkt(output_axis_rule=False)}\n")
    print(f"WKT(axis): {ppsrs.to_wkt(output_axis_rule=True)}\n")

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

    crs = pyproj.CRS.from_json(srs)

    # proper JSON formatting for Bounds object with CRS
    bb = json.dumps({
            "minx": bnds.minx, 
            "miny": bnds.miny, 
            "maxx": bnds.maxx, 
            "maxy": bnds.maxy,
            "crs": crs.to_wkt()
            # "crs": pyproj.CRS.from_json(srs).to_wkt()
        }, 
        indent = 4,
        sort_keys = False)
    print(bb)

    file = open("../TestOutput/bounds.json", "w")

    # Write data to the file
    file.write(bb)

    # Close the file
    file.close()

if testnum() == 4:      # only run if asked
    # reproject NOAA data for Wrangell Island, AK from geographic to UTM zone 7
    inFolder = "H:/NOAATestData"
    outFolder = "H:/NOAATestData/UTM7"
    # get list of COPC assets in data folder...could also be a list of URLs
    assets = [fn.as_posix() for fn in Path(inFolder).glob("*.copc.laz")]

    for asset in assets:
        # build pipeline
        print(f"{asset} to {asset.replace(inFolder, outFolder)}")
        p = pdal.Reader(asset)
        p |= pdal.Filter.reprojection(out_srs = "EPSG:26907", in_axis_ordering = "2,1", error_on_failure = True)
        p |= pdal.Writer.copc(asset.replace(inFolder, outFolder))

        # execute
        p.execute()

if testnum() == 5:      # only run if asked
    #inFolder = "H:/NOAATestData"
    #inFolder = 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045'
    inFolder = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/AK_SouthEastLandslides_D22/AK_SELandslides_1_D22/LAZ/'
    pattern = "*.laz"

    assets = inventory_assets(inFolder, pattern)

    print(assets)

if testnum() == 6:      # only run if asked
    # testing for assetCatalog class with various sources for data
    #inFolder = "H:/NOAATestData"
    inFolder = "H:/NOAATestData/UTM7"
    #inFolder = "H:/FSTestData/R6_noCRS"
    #inFolder = "H:/FSTestData/R6_hasCRS"
    #inFolder = "T:/FS/Reference/RSImagery/ProcessedData/r06/R06_DRM_Deliverables/PointCloud/OKW_2021_MinerCreek/1_LAZ/"
    pattern = "*.copc.laz"
    #pattern = "*.laz"
    assets = [fn.as_posix() for fn in Path(inFolder).glob(pattern)]

    # rockyweb is VERY slow, tested this with just 3 files and it took 10+ minutes to read the headers
    # use with scanheaders = False to just get file names
    #baseURL = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ'
    #pattern = ".laz"

    # ****** NOAA s3 data doesn't allow directory listing
    #baseURL = 'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045'

    # NOAA bulk download page...this works but you must use href_asis option when creating the catalog because the page has
    # full URLs for each data file
    baseURL = "https://noaa-nos-coastal-lidar-pds.s3.amazonaws.com/laz/geoid12b/8539/index.html"        # 64 files
    baseURL = "https://noaa-nos-coastal-lidar-pds.s3.amazonaws.com/laz/geoid12b/9577/index.html"        # smaller project with 17 files
    #pattern = ".laz"

    # for ept, pass the ept.json file as assets = [baseURL]...base and pattern should be "" but can be anything
    baseURL = "https://noaa-nos-coastal-lidar-pds.s3.amazonaws.com/entwine/geoid12b/9577/ept.json"
    #pattern = "ept.json"

    assets = [
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_664000_6243000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6244000.copc.laz',
        'https://noaa-nos-coastal-lidar-pds.s3.us-east-1.amazonaws.com/laz/geoid12b/10045/20230707_TNFWI_665000_6243000.copc.laz'
        ]

    assets = [
        'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ/USGS_LPC_WA_ElwhaRiver_2015_10UDU702317.laz',
        'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ/USGS_LPC_WA_ElwhaRiver_2015_10UDU702310.laz',
        'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ/USGS_LPC_WA_ElwhaRiver_2015_10UDU695310.laz',
        'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ/USGS_LPC_WA_ElwhaRiver_2015_10UDU695317.laz',
        'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WA_ElwhaRiver_2015/WA_Elwha_TB_2015/LAZ/USGS_LPC_WA_ElwhaRiver_2015_10UDU687317.laz'
    ]
    
    #cat = assetCatalog("", "", assets=[baseURL])
    #cat = assetCatalog(inFolder, pattern, assets=assets)
    cat = assetCatalog(inFolder, pattern, testsrs=True)

    # for ept data sources, provide empty strings for base and pattern, URLs for ept.json files as a list of assets,
    # and set assettype to 'ept'. The ept.json file will be read with PDAL quickinfo to get minimal header information.
    # Then the header of the root volume file (0-0-0-0.laz) will be read to get more information.
    #
    # For some servers, you may be able to provide the base URL in base and 'ept.json' in pattern (with assettype = 'ept').
    # For s3, this won't work because you can't get a directory listing using a get request.
    # assets = [
    #     "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/WI_Oshkosh_3Rivers_FondDuLac_TL_2018/ept.json",
    #     "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/NM_SouthCentral_Fort_Sill_TL_2018/ept.json"
    #           ]

    # cat = assetCatalog("", "", assets, assettype='ept')
    # doesn't work for s3: cat = assetCatalog("https://s3-us-west-2.amazonaws.com/usgs-lidar-public/WI_Oshkosh_3Rivers_FondDuLac_TL_2018", "ept.json", assettype='ept')

    #cat = assetCatalog(baseURL, pattern, scanheaders=True, href_asis=True)
    
    cat.print(srs=False)

    #cat.to_file("assets.parquet")
    cat.to_file("assets.gpkg", content='all')
    #cat.to_file("assets.geojson")
    #cat.to_file("assets.gpkg")

    # iterate over assets
    #if cat.is_valid():
    #    for asset in cat.assets:
    #        print(asset.filename)

if testnum() == 7:
    inFolder = "T:/FS/Reference/RSImagery/ProcessedData/r06/R06_DRM_Deliverables/PointCloud/COL_2008_Sherman_Pass_feet/1_LAZ"
    pattern = "*.laz"
    fileName = "COL_2008_48118E2101_Sherman_Pass.laz"
    
    cat = assetCatalog(inFolder, pattern)
    cat.print(details = False)
    cat.to_file("assets.gpkg", content='all')

# read folder list for T: drive and build index files
if testnum() == 8:
    slashString = "_][_"
    rootPath = "T:\\FS\\Reference\\RSImagery\\ProcessedData\\r06\\R06_DRM_Deliverables\\PointCloud"

    # copied file to same folder with code. Didn't like having file in C:\users
    csvList = pandas.read_csv(Path("TDrive_R6_FileList.csv"))

    # add column with the sum of the data type flags...non-zero indicates folder has target file types
    csvList['typeSum'] = csvList['*.laz'] + csvList['*.las'] + csvList['*.lda'] + csvList['ept.json'] + csvList['*.copc'] + csvList['*.copc.laz']
    csvList['indexName'] = ""

    # sort by *.laz and *.las columns, then by folder name
    csvList.sort_values(['*.laz', '*.las', 'Folder'], ascending = [False, False, True])

    # drop rows that don't have target file types
    finalList = csvList[(csvList['typeSum'] > 0)]

    finalList = finalList.reset_index(drop = True)

    # manipulate folder name to get name for index
    # idea is to strip off root path, then replace slashes with "__"
    # root path...Path() will replace slashes and delete training slash

    finalList.loc[:, 'indexName'] = finalList.loc[:, 'Folder'].str.replace(rootPath, "").str.replace("\\", slashString).str.lstrip(slashString)
    
    # indexName has the folder name...iterate over rows
    for index, row in finalList.iterrows():
        print("Scanning:", row['Folder'], "to produce index:", row['indexName'], "...", end = "")

        # do index

        print("Done!\n")

        if index > 30:
            break

    #pandas.set_option('display.max_rows', 25)
    #print(finalList)
    #print(finalList.iloc[0, 8])
