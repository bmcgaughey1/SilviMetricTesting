# link for info regarding tile indexes in PDAL
# https://pdal.io/en/latest/tutorial/tindex/index.html
#
###############################################################################    
############## Helper functions for SilviMetric workflows #####################
###############################################################################    
import os
import sys
from pathlib import Path
import pdal
import json
import pyproj

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import requests
import warnings

from silvimetric import Bounds

import geopandas as gpd
from shapely.geometry import box

###############################################################################    
############################  C L A S S E S  ##################################
###############################################################################    
class assetInfo:
    """Represents point files/URLs or raster layers along with their bounds
    and srs."""
    def __init__(self
                 , filename: str = ""
                 , bounds: Bounds = None
                 , numpoints: int = 0
                 , srs: str = ""
                 ):
        self.filename = filename
        """asset filename or URL"""
        self.bounds = bounds
        """bounding box for asset...comes from point file header"""
        self.numpoints = numpoints
        """number of points in asset"""
        self.srs = srs
        """srs for asset...comes from point file header"""

class assetCatalog:
    """Collection of assets along with overall information"""
    def __init__(self
                 , base: str
                 , pattern: str
                 , assets: list[str] = []
                 , assettype: str = "points"
                 , overallbounds: Bounds = None
                 , totalpoints: int = 0
                 , srs: str = ""
                 , testsrs: bool = True
                 , testtype: str = "string"
                 , srsmatch: bool = False
                 , scanheaders: bool = True
                 , href_asis: bool = False
                 ):
        self.base = base
        """Folder name or URL containing assets"""
        self.pattern = pattern
        """Filename template. Can include wildcards for local files (e.g. '*.copc.laz') 
        or simple extension (e.g. '.copc.laz') for URLs."""
        self.assets = assets
        """List of filenames/URLs"""
        self.assettype = assettype
        """Type of asset: 'points' or 'raster'"""
        self.overallbounds = overallbounds
        """Overall bounding box covering all assets"""
        self.totalpoints = totalpoints
        """Total number of points in all assets"""
        self.srs = srs
        """srs for asset collection"""
        self.testsrs = testsrs
        """Did we test for same srs across all assets?"""
        self.testtype = testtype
        """Test method for srs. Options are 'string' or 'pyproj'."""
        self.srsmatch = srsmatch
        """Did srs match for all assets?"""
        self.scanheaders = scanheaders
        """Were headers scanned?"""
        self.href_asis = href_asis
        """Were hrefs from URL left as is (not prepended with base URL)?"""

        # scan assets
        if not self.__scan_assets():
            raise Exception(f"No assets found in {base} matching {pattern}")

    def is_valid(self) -> bool:
        """
        Test to see if catalog is valid (has assets, headers were scanned, 
        srs matched for all assets).
        """
        
        return self.has_assets() and self.scanheaders and self.srsmatch

    def has_assets(self) -> bool:
        """
        Test to see if catalog has assets.
        """
        if len(self.assets) > 0:
            if isinstance(self.assets[0], assetInfo):
                return True
            
        return False
        
    def headers_scanned(self) -> bool:
        """
        Returns whether or not asset headers were scanned.
        """
        return self.scanheaders
    
    def print(self
              , filename: bool = True
              , bounds: bool = True
              , numpoints: bool = True
              , srs: bool = False
              ):
        """
        Pretty-print catalog assets.
        """
        if self.has_assets():
            print(f"Catalog of: {self.base} matching {self.pattern}")
            print(f"Overall bounding box: {self.overallbounds}")
            print(f"Total number of points: {self.totalpoints}")
            print(f"Coordinate system information: {self.srs}")
            print(f"Number of assets: {len(self.assets)}")

            if filename or bounds or srs:
                cnt = 1
                for asset in self.assets:
                    print(f"   Asset {cnt}:")
                    if filename: print(f"      {asset.filename}")
                    if bounds: print(f"      {asset.bounds}")
                    if numpoints: print(f"      {asset.numpoints}")
                    if srs: print(f"      {asset.srs}")

                    cnt = cnt + 1
        else:
            print("No assets to print")

    def update_overall_bounds(self) -> Bounds:
        """
        Update the overall bounding box for the assets in catalog.
        """
        if self.is_valid():
            bb = Bounds(sys.float_info.max, sys.float_info.max, -sys.float_info.max, -sys.float_info.max)

            # Update overall bounds with asset bounds
            for asset in self.assets:
                # compare bounds
                if asset.bounds.minx < bb.minx:
                    bb.minx = asset.bounds.minx
                if asset.bounds.miny < bb.miny:
                    bb.miny = asset.bounds.miny
                if asset.bounds.maxx > bb.maxx:
                    bb.maxx = asset.bounds.maxx
                if asset.bounds.maxy > bb.maxy:
                    bb.maxy = asset.bounds.maxy

            return bb
        
        return None
    
    def to_file(self
               , filename: str
               ) -> bool:
        """
        Write catalog to file. Supports geoparquet, shapfile, geopackage, geojson and json (written as geojson).
        
        :raises ValueError: Unsupported file type

        Returns:
            bool, True if successful
        """
        if self.is_valid():
            data = {
                'filename': [asset.filename for asset in self.assets],
                'numpoints': [asset.numpoints for asset in self.assets],
                'geometry': [box(asset.bounds.minx, asset.bounds.miny, asset.bounds.maxx, asset.bounds.maxy) for asset in self.assets]
            }

            gdf = gpd.GeoDataFrame(data, crs=self.srs)

            if filename.lower().endswith(".parquet"):
                gdf.to_parquet(filename)
            elif filename.lower().endswith(".shp"):
                gdf.to_file(filename, crs = self.srs)
            elif filename.lower().endswith(".geojson") or filename.lower().endswith(".json"):
                gdf.to_file(filename, driver = 'geoJSON', crs = self.srs)
            elif filename.lower().endswith(".gpkg"):
                gdf.to_file(filename, driver = 'GPKG', layer = 'assets', crs = self.srs)
            else:
                raise ValueError(f"Format not supported: {filename}")
            return True
        
        return False

    def __test_assets_srs(self
                    , testtype: str = "string"
                    ) -> bool:
        """
        Test that all assest have same srs.
        """
        if self.has_assets():
            crs = pyproj.CRS.from_json(self.assets[0].srs)
            for i in range(1, len(self.assets)):
                fcrs = pyproj.CRS.from_json(self.assets[i].srs)

                if testtype.lower() == 'string':
                    if crs.to_wkt().lower() != fcrs.to_wkt().lower():
                        return False
                else:
                    if not crs.is_exact_same(fcrs):
                        return False
            
            return True
        else:
            return False
            
    def __scan_assets(self) -> int:
        """
        Build a list of assets including srs and bounding box.
        
        :raises ValueError: unsupported asset type
        :raises Exception: assest has no coordinate system information

        Returns:
            integer, number of assets found
        """
        # if assets is not empty, assume assets was passed as a list of strings so we can skip the directory listing
        if not len(self.assets):
            # see if we have URL with http or https
            if 'http' in self.base.lower():
                warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
                def listFD(url, ext=''):
                    if url.endswith("/"):
                        url = url[:-1]

                    page = requests.get(url).text
                    soup = BeautifulSoup(page, 'html.parser')

                    if not self.href_asis:
                        return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
                    else:
                        return [node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

                tassets = listFD(self.base, self.pattern.replace("*", ""))
            else:
                tassets = [fn.as_posix() for fn in Path(self.base).glob(self.pattern)]
        else:
            tassets = self.assets

        # read header and get bounding box and srs
        if len(tassets) and self.scanheaders:
            self.assets = []
            for ta in tassets:
                # use PDAL python bindings to find the srs of our data...look at first asset
                if self.assettype == 'points':
                    reader = pdal.Reader(ta)
                    p = reader.pipeline()
                    qi = p.quickinfo[reader.type]
                    srs = json.dumps(qi['srs']['json'])
                    b = Bounds.from_string((json.dumps(qi['bounds'])))
                    np = int(json.dumps(qi['num_points']))
                else:
                    raise ValueError(f"{self.assettype} type not supported!")
                
                if len(srs) == 0:
                    raise Exception(f"Asset {ta} does not have srs")
                
                self.assets.append(assetInfo(ta, b, np, srs))

            self.overallbounds = self.update_overall_bounds()
            self.srs = self.assets[0].srs
            self.__sum_points()

            if (self.testsrs):
                self.srsmatch = self.__test_assets_srs(testtype = self.testtype)
        elif len(tassets):
            for ta in tassets:
                self.assets.append(assetInfo(ta, bounds = None, numpoints = 0, srs = ""))

            self.overallbounds = None
            self.totalpoints = 0

        return len(self.assets)

    def __sum_points(self) -> int:
        self.totalpoints = 0
        if self.is_valid():
            for asset in self.assets:
                self.totalpoints = self.totalpoints + asset.numpoints

            return self.totalpoints
        
        return 0
    
