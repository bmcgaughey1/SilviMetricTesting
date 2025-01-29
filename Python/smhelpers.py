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
# scan a list of assets for srs info. Optionally check that all assets
# in list have same srs.
#
# Returns valid srs if successful, empty string otherwise
def scan_for_srs(assets: list[str], all_must_match = True) -> str:
    """Use PDAL quickinfo to get srs for first asset in list of assets. Optionally,
    check that all assets in list have same srs.

    :raises Exception: srs for assets in list are different from srs for first asset
    :return: srs string
    """

    if len(assets) == 0:
        return ""
        
    # use PDAL python bindings to find the srs of our data...look at first asset
    reader = pdal.Reader(assets[0])
    p = reader.pipeline()
    qi = p.quickinfo[reader.type]
    srs = json.dumps(qi['srs']['json'])
    
    # check for matching SRS for all assets...case insensitive
    if all_must_match:
        for i in range(1, len(assets)):
            reader = pdal.Reader(assets[i])
            p = reader.pipeline()
            qi = p.quickinfo[reader.type]
            fsrs = json.dumps(qi['srs']['json'])
            if fsrs.lower() != srs.lower():
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
def scan_for_bounds(assets: list[str], resolution: float | int, adjust_to_cell_lines = False) -> Bounds:
    """Use PDAL quickinfo to get overall bounding box for data in a list of assets.

    :raises Exception: List of assets is empty
    :return: Return SilviMetric Bounds object optionally, adjusted to cell lines
    """

    # check for assets
    if not len(assets):
        raise Exception("List of assets is empty")
    
    # bogus bounds to start
    bounds = Bounds(sys.float_info.max, sys.float_info.max, sys.float_info.min, sys.float_info.min)

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

    if adjust_to_cell_lines:
        bounds.adjust_to_cell_lines(resolution)
        
    return bounds

###### Build pipeline used to get data ######
# This allows us to read individual assets and shatter them. Pipeline
# does some simple filtering using classification values and flags.
# Pipeline also normalizes points using VRT and does simple HAG filtering.
# Filtering is done in two steps. First using classification values
# and flags, then, after normalization, using HAG. This reduces number
# of points being normalized.
def build_pipeline(asset_filenme: str
                    , add_classes: list[int] = []
                    , skip_classes: list[int] = []
                    , skip_synthetic = True
                    , skip_keypoint = False
                    , skip_withheld = True
                    , skip_overlap = False
                    , override_srs: str = ""
                    , do_HAG = False
                    , ground_VRT: str = ""
                    , min_HAG: float = -5.0
                    , max_HAG: float = 150.0
                    , out_srs: str = ""
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
    stage = pdal.Reader(asset_filenme)

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
def write_pipeline(p, filename: str) -> None:
    """Write pipeline

    :return: None
    """

    try:
        with open(filename, 'w') as f:
            try:
                f.write(p.pipeline)
            except (IOError, OSError):
                f.close()
                print("Error writing to file")
    except (FileNotFoundError, PermissionError, OSError):
        print("Error opening file")

    #f = open(filename, "w")
    #f.write(p.pipeline)
    #f.close()

