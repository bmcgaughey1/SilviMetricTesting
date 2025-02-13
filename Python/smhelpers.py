# link for info regarding tile indexes in PDAL
# https://pdal.io/en/latest/tutorial/tindex/index.html
#
###############################################################################    
############## Helper functions for SilviMetric workflows #####################
###############################################################################    
import os
import sys
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree
from osgeo import gdal, osr
import pyproj

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import sm_min, sm_max, mean
# from silvimetric.resources.metrics.__init__ import grid_metrics

###############################################################################    
##########################  F U N C T I O N S  ################################
###############################################################################    
###### transform Bounds object ######
def transform_bounds(b: Bounds, in_srs: str, out_srs: str, edge_samples: int = 11) -> Bounds:
    """Transform SilviMetric Bounds object from in_srs to out_srs.
    
    This transform accounts for the fact that the reprojected square bounding
    box might be warped in the new coordinate system.  To account for this,
    the function samples points along the original bounding box edges and
    attempts to make the largest bounding box around any transformed point
    on the edge whether corners or warped edges.

    Parameters:
        b (Bounds): a SilviMetric Bounds object in `in_srs` coordinate
            system describing the bounding box
        in_srs (str): srs of the input coordinate system (PROJJSON format)
        out_srs (str): srs of the desired output coordinate system  (PROJJSON format)
        edge_samples (int): the number of interpolated points along each
            bounding box edge to sample along. A value of 2 will sample just
            the corners while a value of 3 will also sample the corners and
            the midpoint.

    :raises Exception: Can't create CoordinateTransformation using 'in_srs' and 'out_srs'
    :raises Exception: TransformBounds fails
    :raises Exception: Transformed bounding box is invalid.

    Returns:
        A SilviMetric Bounds object that describes the largest
        fitting bounding box around the original warped bounding box in
        `out_srs` coordinate system.
    """
    osr.UseExceptions()

    # create SpatialReference for in_srs and out_srs
    # This is awkward since I am only using pyproj to read the PROJJSON srs
    # and convert to WKT for SpatialReference
    #
    # example code for doing transform in pyproj
    # https://pyproj4.github.io/pyproj/stable/api/transformer.html
    ppcrs = pyproj.CRS.from_json(in_srs)
    in_sr = osr.SpatialReference(ppcrs.to_wkt())

    # get EPSG for out_srs
    ppcrs = pyproj.CRS.from_json(out_srs)
    out_sr = osr.SpatialReference(ppcrs.to_wkt())

    # build transformer
    try:
        transformer = osr.CoordinateTransformation(in_sr, out_sr)
    except:
        raise Exception(f"Could not create CoordinateTransformation using \nin_srs:\n{in_srs} and \nout_srs:\n{out_srs}")
    
    bb = list()

    # transform bounding box and try to deal with lat-lon and lon-lat
    try:
        if in_sr.IsGeographic():
            if in_sr.GetAxisName('GEOGCS', 0).lower() == 'latitude':
                bb = list(transformer.TransformBounds(b.miny, b.minx, b.maxy, b.maxx, edge_samples))
            else:
                bb = list(transformer.TransformBounds(b.minx, b.miny, b.maxx, b.maxy, edge_samples))
        else:
            bb = list(transformer.TransformBounds(b.minx, b.miny, b.maxx, b.maxy, edge_samples))
    except:
        raise Exception("Could not transform bounding box")
    
    if len(bb) == 0:
        raise Exception("Transformed bounding box is invalid...check input and output srs")
    
    # build Bounds object to return
    tb = Bounds(bb[0], bb[1], bb[2], bb[3])
        
    return tb
    
###### scan for srs ######
# scan a list of assets for srs info. Optionally check that all assets
# in list have same srs.
#
# Returns valid srs in PROJJSON formatif successful, empty string otherwise
#
# testtype can be 'string' for character by character test or 'pyproj' for
# result for pyproj.CRS.is_exact_same()
def scan_for_srs(assets: list[str], all_must_match: bool = True, testtype: str = 'string') -> str:
    """Use PDAL quickinfo to get srs for first asset in list of assets. Optionally,
    check that all assets in list have same srs.

    :raises Exception: List of assets is empty
    :raises Exception: srs for assets in list are different from srs for first asset
    
    :return: srs string in PROJJSON format
    """

    if len(assets) == 0:
        raise Exception(f"list of assets is empty")
        
    # use PDAL python bindings to find the srs of our data...look at first asset
    reader = pdal.Reader(assets[0])
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    srs = json.dumps(qi['srs']['json'])
    
    # check for matching SRS for all assets...case insensitive
    # the 'string' test is based on a simple string comparison. Given that you can define
    # the same projection in multiple ways, the test is far from infallible!!
    #
    # the 'pyproj' test should be more robust because it creates a more standardized CRS
    # from the json srs and then tests the new CRS descriptions.
    if all_must_match:
        crs = pyproj.CRS.from_json(srs)
        for i in range(1, len(assets)):
            reader = pdal.Reader(assets[i])
            p = reader.pipeline()
            qi = p.quickinfo[reader.type]
            fsrs = json.dumps(qi['srs']['json'])
            fcrs = pyproj.CRS.from_json(fsrs)

            if testtype.lower() == 'string':
                if srs.lower() != fsrs.lower():
                    raise Exception(f"srs for asset: {assets[i]} ({fsrs}) does not match srs for first asset: {assets[0]} ({srs})")
            else:
                if not crs.is_exact_same(fcrs):
                    raise Exception(f"srs for asset: {assets[i]} ({fsrs}) does not match srs for first asset: {assets[0]} ({srs})")
                
    return srs

###### scan an individual asset for bounding box ######
# Use PDAL quickinfo to get bounding box
#
# returns silvimetric.resources.bounds.Bounds object
def scan_asset_for_bounds(asset: str) -> Bounds:
    """Use PDAL quickinfo to get bounding box for data in asset.

    :return: Return SilviMetric Bounds object
    """

    reader = pdal.Reader(asset)
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    fb = Bounds.from_string((json.dumps(qi['bounds'])))
    
    return fb
    
###### scan for bounding box for a set of point assets ######
# Use PDAL quickinfo to get bounding box for each asset and build an overall bounding box.
# Optionally adjust (expand) bounding box to fall on cell boundaries.
#
# Probably don't want to adjust bounding box since the adjustment will happen again when the 
# storage is created. adjust_to_lines() modified to expand bounds by 1/2 cell to match
# FUSION alignment will add a cell each time it is called.
#
# returns silvimetric.resources.bounds.Bounds object
def scan_for_bounds(assets: list[str], resolution: float | int = 0, adjust_alignment = False, alignment = 'pixelispoint') -> Bounds:
    """Use PDAL quickinfo to get overall bounding box for data in a list of assets.

    :raises Exception: List of assets is empty
    :raises Exception: Resolution is invalid (<= 0) and adjust_alignment == True

    :return: Return SilviMetric Bounds object optionally, adjusted to cell lines
    """

    # check for assets
    if len(assets) == 0:
        raise Exception("List of assets is empty")
    
    # check resolution
    if adjust_alignment and resolution <= 0:
        raise Exception(f"Invalid resolution: {resolution}")
    
    # bogus bounds to start
    bounds = Bounds(sys.float_info.max, sys.float_info.max, -sys.float_info.max, -sys.float_info.max)

    # use PDAL python bindings to get bounds for each tile and update overall bounds
    for asset in assets:
        fb = scan_asset_for_bounds(asset)

        # compare bounds
        if fb.minx < bounds.minx:
            bounds.minx = fb.minx
        if fb.miny < bounds.miny:
            bounds.miny = fb.miny
        if fb.maxx > bounds.maxx:
            bounds.maxx = fb.maxx
        if fb.maxy > bounds.maxy:
            bounds.maxy = fb.maxy

    if adjust_alignment:
        bounds.adjust_alignment(resolution, alignment)
        
    return bounds

###### Build pipeline used to get data ######
# This allows us to read individual assets and shatter them. Pipeline
# does some simple filtering using classification values and flags.
# Pipeline also normalizes points using VRT and does simple HAG filtering.
# Filtering is done in two steps. First using classification values
# and flags, then, after normalization, using HAG. This reduces number
# of points being normalized.

# using hag_nn and hag_delaunay methods may be problematic given we will
# be getting points cell by cell so ground points may be sparse or poorly
# distributed.

def build_pipeline(asset: str
                    , add_classes: list[int] = []
                    , skip_classes: list[int] = []
                    , skip_synthetic = True
                    , skip_keypoint = False
                    , skip_withheld = True
                    , skip_overlap = False
                    , override_srs: str = ""
                    , HAG_method: str | None = None       # choices: "delaunay", "nn", "dem", "vrt"..."dem" and "vrt" are equilvalent
                    , ground_VRT: str = ""
                    , min_HAG: float = -5.0
                    , max_HAG: float = 150.0
                    , out_srs: str = ""
                    , HAG_replaces_Z = False
                   ):
    """Create pipeline to feed points to SilveMetric

    :raises Exception: The same classes are included in add_classes and skip_classes
    :raises Exception: Invalid value for HAG_method. Valid choices: "delaunay", "nn", "dem", "vrt"..."dem" and "vrt" are equilvalent.
    "dem" and "vrt" both expect a filename in ground_VRT that is either a single raster or a VRT file name.

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
    stage = pdal.Reader(asset)

    # override srs for points...
    if override_srs != "":
        stage._options['override_srs'] = f"{override_srs}"

    # build pipeline
    p = pdal.Pipeline([stage])

    if len(exp) > 0:
        p |= pdal.Filter.expression(expression = exp)
    
    # assumes DEM VRT and point data use same srs
    if HAG_method != None:
        if HAG_method.lower() == "delaunay":
            p |= pdal.Filter.hag_delaunay(allow_extrapolation = True)
        elif HAG_method.lower() == "nn":
            p |= pdal.Filter.hag_nn(allow_extrapolation = True)
        elif HAG_method.lower() in ["dem", "vrt"]:
            p |= pdal.Filter.hag_dem(raster = ground_VRT, zero_ground = False)
        else:
            raise Exception(f"Invalid choice for HAG_method: {HAG_method}. Valid choices are delaunay, nn, dem, or vrt.")
        p |= pdal.Filter.expression(expression = f"HeightAboveGround >= {min_HAG} && HeightAboveGround <= {max_HAG}")
    
    # do projection after HAG so we can use source DEM VRT
    if out_srs != "":
        p |= pdal.Filter.reprojection(out_srs = f"{out_srs}", in_srs = f"{override_srs}")

    # replace Z with HAG
    if HAG_method != None and HAG_replaces_Z:
        p |= pdal.Filter.ferry(dimensions = "HeightAboveGround=>Z")

    # return pipeline
    return p

###### write pipeline file ######
# Write pipeline to json file
def write_pipeline(p, filename: str) -> None:
    """Write pipeline

    :return: None
    """

    try:
        with open(filename, 'w') as f:
            try:
                f.write(p.pipeline)
            except (IOError, OSError):
                print("Error writing to file")
    except (PermissionError, OSError):
        print("Error opening file")

