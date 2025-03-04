# link for info regarding tile indexes in PDAL
# https://pdal.io/en/latest/tutorial/tindex/index.html
#
###############################################################################    
############## Helper functions for SilviMetric workflows #####################
###############################################################################
#
# Information available from PDAL's quickinfo command is limited and does
# not represent all information available in point file headers. I have older
# code in R that reads and captures all header information when creating
# index files.
#
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
    """
    Represents point files/URLs or raster layers along with their bounds
    and srs.
    """
    def __init__(self
                 , filename: str = ""
                 , bounds: Bounds = None
                 , numpoints: int = 0
                 , srs: str = ""
                 , compressed: bool = False
                 , copc: bool = False
                 , creation_doy: int = 0
                 , creation_year: int = 0
                 , point_record_format: int = -1
                 , major_version: int = 0
                 , minor_version: int = 0
                 ):
        self.filename = filename
        """asset filename or URL"""
        self.bounds = bounds
        """bounding box for asset...comes from point file header"""
        self.numpoints = numpoints
        """number of points in asset"""
        self.srs = srs
        """srs for asset...comes from point file header"""
        self.compressed = compressed
        """Is file compressed?"""
        self.copc = copc
        """Is file COPC format?"""
        self.creation_doy = creation_doy
        """Day of year of file creation"""
        self.creation_year = creation_year
        """Year of file creation"""
        self.point_record_format = point_record_format
        """Point data record type"""
        self.major_version = major_version
        """LAS major version"""
        self.minor_version = minor_version
        """LAS minor version"""

        if srs != "":
            self.hassrs = True
        else:
            self.hassrs = False

class assetCatalog:
    """
    Collection of assets along with overall information.
    """
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
                 , isremote: bool = False
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
        self.isremote = isremote
        """Does base resolve to a remote host?"""

        # scan assets
        if not self.__scan_assets():
            raise Exception(f"No assets found in {base} matching {pattern}")

    def is_complete(self) -> bool:
        """
        Test to see if catalog is complete (has assets and srs, headers were scanned, 
        srs matched for all assets).
        """
        
        return self.has_assets() and self.scanheaders and self.srsmatch

    def is_valid(self) -> bool:
        """
        Test to see if catalog is valid (has assets and headers were scanned).
        """
        
        return self.has_assets() and self.scanheaders

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
              , assetsrs: bool = False
              , details:bool = True
              ):
        """
        Pretty-print catalog assets.
        """
        if self.has_assets():
            print(f"Catalog of: {self.base} matching {self.pattern}")
            print(f"Overall bounding box: {self.overallbounds}")
            print(f"Total number of points: {self.totalpoints}")
            if srs: print(f"Coordinate system information: {self.srs}")
            print(f"Number of assets: {len(self.assets)}")

            if filename or bounds or srs:
                cnt = 1
                for asset in self.assets:
                    print(f"   Asset {cnt}:")
                    if filename: print(f"      {asset.filename}")
                    if bounds: print(f"      bounds: {asset.bounds}")
                    if numpoints: print(f"      numpoints: {asset.numpoints}")
                    if assetsrs: print(f"      srs: {asset.srs}")
                    if details:
                        print(f"      compressed: {asset.compressed}")
                        print(f"      copc: {asset.copc}")
                        print(f"      creation_doy: {asset.creation_doy}")
                        print(f"      creation_year: {asset.creation_year}")
                        print(f"      point_record_format: {asset.point_record_format}")
                        print(f"      major_version: {asset.major_version}")
                        print(f"      minor_version: {asset.minor_version}")

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
                                            #  , compressed
                                            #  , copc
                                            #  , creation_doy
                                            #  , creation_year
                                            #  , point_record_format
                                            #  , major_version
                                            #  , minor_version))

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
            if self.assets[0].hassrs:
                crs = pyproj.CRS.from_json(self.assets[0].srs)
                for asset in self.assets[1:]:
                    fcrs = pyproj.CRS.from_json(asset.srs)

                    if testtype.lower() == 'string':
                        if crs.to_wkt().lower() != fcrs.to_wkt().lower():
                            return False
                    else:
                        if not crs.is_exact_same(fcrs):
                            return False
                
                return True
            else:
                return False
        else:
            return False
            
    def __scan_assets(self) -> int:
        """
        Build a list of assets including srs and bounding box. Uses PDAL
        quickinfo to get header information so the entire file header is
        not available.
        
        :raises ValueError: unsupported asset type
        :raises Exception: assest has no coordinate system information

        Returns:
            integer, number of assets found
        """
        # if assets is not empty, assume assets was passed as a list of strings so we can skip the directory listing
        if len(self.assets) == 0:
            tassets = self.__list_assets()
        else:
            tassets = self.assets
#https://www.google.com/search?q=get+remote+file+size+python&sca_esv=74940b13bb8c626e&ei=eTLGZ-_aNbSiptQPluOooAY&ved=0ahUKEwiv-9D9_-6LAxU0kYkEHZYxCmQQ4dUDCBA&uact=5&oq=get+remote+file+size+python&gs_lp=Egxnd3Mtd2l6LXNlcnAiG2dldCByZW1vdGUgZmlsZSBzaXplIHB5dGhvbjIGEAAYCBgeMggQABiiBBiJBTIFEAAY7wVI4R1QpRRYwxtwAXgBkAEAmAFdoAHyA6oBATe4AQPIAQD4AQGYAgegAsMDwgIKEAAYsAMY1gQYR8ICDRAAGIAEGLADGEMYigXCAgcQABiABBgNwgIIEAAYBxgIGB7CAgYQABgNGB7CAggQABgFGA0YHsICCBAAGAgYDRgemAMAiAYBkAYJkgcBN6AH4Sc&sclient=gws-wiz-serp
#https://www.google.com/search?q=get+file+size+python&sca_esv=74940b13bb8c626e&source=hp&ei=cjLGZ-jbB77C0PEPmr7viAM&iflsig=ACkRmUkAAAAAZ8ZAgqWHR9PvEX_Y5WLatYHnESR6-_d-&ved=0ahUKEwjo3Pf5_-6LAxU-ITQIHRrfGzEQ4dUDCBo&uact=5&oq=get+file+size+python&gs_lp=Egdnd3Mtd2l6IhRnZXQgZmlsZSBzaXplIHB5dGhvbjIFEAAYgAQyBRAAGIAEMgYQABgWGB4yBhAAGBYYHjIGEAAYFhgeMgYQABgWGB4yBhAAGBYYHjIGEAAYFhgeMgYQABgWGB4yBhAAGBYYHkioMlAAWJ0scAB4AJABAJgBeaABlwuqAQQxOC4yuAEDyAEA-AEBmAIUoALiC8ICCxAAGIAEGLEDGIMBwgIREC4YgAQYsQMY0QMYgwEYxwHCAg4QLhiABBixAxiDARiKBcICDhAAGIAEGLEDGIMBGIoFwgIIEAAYgAQYsQPCAgsQLhiABBixAxiDAcICCxAuGIAEGLEDGNQCwgILEC4YgAQYxwEYrwHCAgsQLhiABBjRAxjHAcICCBAuGIAEGLEDwgIOEC4YgAQYsQMY0QMYxwHCAgUQLhiABMICERAuGIAEGLEDGIMBGMcBGK8BmAMAkgcEMTguMqAHuqkB&sclient=gws-wiz

        # use PDAL quickinfo to get bounding box, srs, and number of points
        if len(tassets) and self.scanheaders:
            self.assets = []
            for ta in tassets:
                # use PDAL to read file header (set count=0 in reader options)
                # I don't know how this compares to quickinfo command but quickinfo doesn't read much information
                if self.assettype == 'points':
                    reader = pdal.Reader(ta)
                    reader._options['count'] = 0
                    p = pdal.Pipeline([reader])
                    p.execute()
                    qi = p.metadata
                    #print(json.dumps(qi, indent = 4))
                    try:
                        srs = json.dumps(qi['metadata'][reader.type]['srs']['json'])
                    except:
                        srs = ""
                    b = Bounds(  float(json.dumps(qi['metadata'][reader.type]['minx']))
                               , float(json.dumps(qi['metadata'][reader.type]['miny']))
                               , float(json.dumps(qi['metadata'][reader.type]['maxx']))
                               , float(json.dumps(qi['metadata'][reader.type]['maxy'])))
                    np = int(json.dumps(qi['metadata'][reader.type]['count']))
                    compressed = bool(json.dumps(qi['metadata'][reader.type]['compressed']))
                    copc = bool(json.dumps(qi['metadata'][reader.type]['copc']))
                    creation_doy = int(json.dumps(qi['metadata'][reader.type]['creation_doy']))
                    creation_year = int(json.dumps(qi['metadata'][reader.type]['creation_year']))
                    point_record_format = int(json.dumps(qi['metadata'][reader.type]['dataformat_id']))
                    major_version = int(json.dumps(qi['metadata'][reader.type]['major_version']))
                    minor_version = int(json.dumps(qi['metadata'][reader.type]['minor_version']))
                else:
                    raise ValueError(f"{self.assettype} type not supported!")
                
                # if len(srs) == 0:
                #     raise Exception(f"Asset {ta} does not have srs")
                
                self.assets.append(assetInfo(ta, b, np, srs
                                             , compressed
                                             , copc
                                             , creation_doy
                                             , creation_year
                                             , point_record_format
                                             , major_version
                                             , minor_version))

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

    def __scan_assets_quickinfo(self) -> int:
        """
        Build a list of assets including srs and bounding box. Uses PDAL
        quickinfo to get header information so the entire file header is
        not available.
        
        :raises ValueError: unsupported asset type
        :raises Exception: assest has no coordinate system information

        Returns:
            integer, number of assets found
        """
        # if assets is not empty, assume assets was passed as a list of strings so we can skip the directory listing
        if len(self.assets) == 0:
            tassets = self.__list_assets()
        else:
            tassets = self.assets

        # use PDAL quickinfo to get bounding box, srs, and number of points
        if len(tassets) and self.scanheaders:
            self.assets = []
            for ta in tassets:
                # use PDAL python bindings to find the srs of our data...look at first asset
                if self.assettype == 'points':
                    reader = pdal.Reader(ta)
                    p = reader.pipeline()
                    qi = p.quickinfo[reader.type]
                    try:
                        srs = json.dumps(qi['srs']['json'])
                    except:
                        srs = ""
                    b = Bounds.from_string((json.dumps(qi['bounds'])))
                    np = int(json.dumps(qi['num_points']))
                else:
                    raise ValueError(f"{self.assettype} type not supported!")
                
                # if len(srs) == 0:
                #     raise Exception(f"Asset {ta} does not have srs")
                
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

    def __is_base_remote(self) -> bool:
        """
        Checks to see if base resolves to a remote host by looking for
        'http' in base. Test is not infalliable.
        
        Returns:
            boolean: True if base is a remote host, False otherwise.
        """

        if 'http' in self.base.lower():
            self.isremote = True
        else:
            self.isremote = False

    def __list_assets(self) -> list[str]:
        """
        List assets using base and pattern.

        Returns:
            List of asset URLs or file specifiers. Empty list if no matching files found.
        """
        # see if we have URL with http or https
        if self.__is_base_remote():
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

        return tassets

    def __sum_points(self) -> int:
        self.totalpoints = 0
        if self.is_valid():
            for asset in self.assets:
                self.totalpoints = self.totalpoints + asset.numpoints

            return self.totalpoints
        
        return 0
    
