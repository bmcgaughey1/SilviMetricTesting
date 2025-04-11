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
from datetime import datetime

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import requests
import warnings
import pystac
from pystac.extensions import pointcloud

from silvimetric import Bounds

import geopandas as gpd
from shapely.geometry import mapping, box

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
                 , filesize: int = -1
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
        self.filesize = filesize
        """size of asset in bytes"""
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
                 , assetsize: int = -1
                 , overallbounds: Bounds = None
                 , totalpoints: int = 0
                 , assetcount: int = 0
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
        """Type of asset: 'points', 'ept' or 'raster'"""
        self.assetsize = assetsize
        """Total size of all asset files in bytes"""
        self.overallbounds = overallbounds
        """Overall bounding box covering all assets"""
        self.totalpoints = totalpoints
        """Total number of points in all assets"""
        self.assetcount = assetcount
        """Number of assets"""
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
        if self.assetcount > 0:
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
            print(f"Catalog of: '{self.base}' matching '{self.pattern}'")
            print(f"Total size: {int(self.assetsize / 1024 / 1024)} Mb")
            print(f"Overall bounding box: {self.overallbounds}")
            print(f"Total number of points: {self.totalpoints}")
            if srs: print(f"Coordinate system information: {self.srs}")
            print(f"Number of assets: {self.assetcount}")

            if filename or bounds or srs:
                cnt = 1
                for asset in self.assets:
                    print(f"   Asset {cnt}:")
                    if filename: print(f"      {asset.filename}")
                    print(f"      size: {int(asset.filesize / 1024)} Kb")
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
        if len(self.assets) > 0:
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
               , content: str = 'assets'            # 'assets', 'overall', 'all'
               , engine: str = 'fiona'              # 'pyogrio' or 'fiona'
               ) -> bool:
        """
        Write catalog to file. Supports geoparquet, shapfile, geopackage, geojson and json (written as geojson).
        
        :raises ValueError: Unsupported file type

        Returns:
            bool, True if successful
        """
        if self.is_valid():
            odata = {
                'base': [self.base],
                'pattern': [self.pattern],
                'assettype': [self.assettype],
                'assetcount': [self.assetcount],
                'assetsize': [self.assetsize],
                'totalpointcount': [self.totalpoints],
                'hasCRS': [self.srs != ""],
                'minx': [self.overallbounds.minx],
                'miny': [self.overallbounds.miny],
                'maxx': [self.overallbounds.maxx],
                'maxy': [self.overallbounds.maxy],
                'geometry': [box(self.overallbounds.minx, self.overallbounds.miny, self.overallbounds.maxx, self.overallbounds.maxy)]
            }
            data = {
                'filespec': [asset.filename for asset in self.assets],
                'filesize': [asset.filesize for asset in self.assets],
                'pointcount': [asset.numpoints for asset in self.assets],
                'compressed': [asset.compressed for asset in self.assets],
                'copc': [asset.copc for asset in self.assets],
                'creation_doy': [asset.creation_doy for asset in self.assets],
                'creation_year': [asset.creation_year for asset in self.assets],
                'point_record_format': [asset.point_record_format for asset in self.assets],
                'major_version': [asset.major_version for asset in self.assets],
                'minor_version': [asset.minor_version for asset in self.assets],
                'minx': [asset.bounds.minx for asset in self.assets],
                'miny': [asset.bounds.maxx for asset in self.assets],
                'maxx': [asset.bounds.miny for asset in self.assets],
                'maxy': [asset.bounds.maxy for asset in self.assets],
                'geometry': [box(asset.bounds.minx, asset.bounds.miny, asset.bounds.maxx, asset.bounds.maxy) for asset in self.assets]
            }

            if self.srs != "":
                ogdf = gpd.GeoDataFrame(odata, crs=self.srs)
                gdf = gpd.GeoDataFrame(data, crs=self.srs)
            else:
                ogdf = gpd.GeoDataFrame(odata, crs=None)
                gdf = gpd.GeoDataFrame(data, crs=None)

            if filename.lower().endswith(".parquet"):
                if content.lower() == 'assets':
                    gdf.to_parquet(filename)
                else:
                    ogdf.to_parquet(filename)
            elif filename.lower().endswith(".shp"):
                if content.lower() == 'assets':
                    gdf.to_file(filename, crs = self.srs, engine = engine)
                else:
                    ogdf.to_file(filename, crs = self.srs, engine = engine)
            elif filename.lower().endswith(".geojson") or filename.lower().endswith(".json"):
                if content.lower() == 'assets':
                    gdf.to_file(filename, driver = 'geoJSON', crs = self.srs, engine = engine)
                else:
                    ogdf.to_file(filename, driver = 'geoJSON', crs = self.srs, engine = engine)
            elif filename.lower().endswith(".gpkg"):
                if 'all' in content.lower():
                    ogdf.to_file(filename, driver = 'GPKG', layer = 'overall', crs = self.srs, engine = engine)
                if content.lower() == 'assets' or content.lower() == 'all':
                    gdf.to_file(filename, driver = 'GPKG', layer = 'assets', crs = self.srs, engine = engine)
            elif filename.lower().endswith(".stac"):
                # Create a STAC Catalog...not working!@#$%^&*
                catalog = pystac.Catalog(
                    id = "mycatalog",
                    description = "Point data assets")

                # Iterate through the GeoDataFrame and create STAC Items
                for index, row in gdf.iterrows():
                    # Create a STAC Item
                    item = pystac.Item(
                        id=row['filespec'],
                        geometry=mapping(row['geometry']),
                        bbox=mapping(row['geometry']),
                        datetime=datetime.now(),
                        properties={'filespec': row['filespec']}
                    )

                    # Add the Item to the Catalog
                    catalog.add_item(item)

                    item.add_asset("pointcloud", asset=pystac.Asset(href=row['filespec'], media_type='LAS'))
                    
                    print(json.dumps(item.to_dict(), indent=4))

                # Save the catalog to a file
                catalog.normalize_hrefs(self.base)
                catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED, dest_href = filename)
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
                if len(self.assets) == 1:
                    return True
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
        to get header information.
        
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

        # use PDAL to get metadata
        if len(tassets) and self.scanheaders:
            self.assets = []
            for ta in tassets:
                # get file size
                filesize = self.__get_asset_size(ta)

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

                    # deal with empty json for srs
                    if srs == "{}": srs = ""

                    b = Bounds(  float(json.dumps(qi['metadata'][reader.type]['minx']))
                               , float(json.dumps(qi['metadata'][reader.type]['miny']))
                               , float(json.dumps(qi['metadata'][reader.type]['maxx']))
                               , float(json.dumps(qi['metadata'][reader.type]['maxy'])))
                    np = int(json.dumps(qi['metadata'][reader.type]['count']))
                    compressed = (json.dumps(qi['metadata'][reader.type]['compressed'])) == "true"
                    copc = (json.dumps(qi['metadata'][reader.type]['copc'])) == "true"
                    creation_doy = int(json.dumps(qi['metadata'][reader.type]['creation_doy']))
                    creation_year = int(json.dumps(qi['metadata'][reader.type]['creation_year']))
                    point_record_format = int(json.dumps(qi['metadata'][reader.type]['dataformat_id']))
                    major_version = int(json.dumps(qi['metadata'][reader.type]['major_version']))
                    minor_version = int(json.dumps(qi['metadata'][reader.type]['minor_version']))

                    self.assets.append(assetInfo(ta, filesize, b, np, srs
                                                , compressed
                                                , copc
                                                , creation_doy
                                                , creation_year
                                                , point_record_format
                                                , major_version
                                                , minor_version))
                elif self.assettype == 'ept':
                    reader = pdal.Reader(ta)
                    p = reader.pipeline()
                    qi = p.quickinfo[reader.type]
                    #print(json.dumps(qi, indent = 4))
                    try:
                        srs = json.dumps(qi['srs']['json'])
                    except:
                        srs = ""

                    # deal with empty json for srs
                    if srs == "{}": srs = ""

                    b = Bounds.from_string((json.dumps(qi['bounds'])))
                    np = int(json.dumps(qi['num_points']))

                    # attempt to read root volume point tile
                    # if this fails, add minimal info for asset...bounds and total number of points
                    tap = ta.replace("ept.json", "ept-data/0-0-0-0.laz")

                    reader = pdal.Reader(tap)
                    reader._options['count'] = 0
                    p = pdal.Pipeline([reader])
                    try:
                        p.execute()

                        qi = p.metadata
                        #print(json.dumps(qi, indent = 4))
                        # b = Bounds(  float(json.dumps(qi['metadata'][reader.type]['minx']))
                        #            , float(json.dumps(qi['metadata'][reader.type]['miny']))
                        #            , float(json.dumps(qi['metadata'][reader.type]['maxx']))
                        #            , float(json.dumps(qi['metadata'][reader.type]['maxy'])))
                        # np = int(json.dumps(qi['metadata'][reader.type]['count']))
                        compressed = (json.dumps(qi['metadata'][reader.type]['compressed'])) == "true"
                        copc = (json.dumps(qi['metadata'][reader.type]['copc'])) == "true"
                        creation_doy = int(json.dumps(qi['metadata'][reader.type]['creation_doy']))
                        creation_year = int(json.dumps(qi['metadata'][reader.type]['creation_year']))
                        point_record_format = int(json.dumps(qi['metadata'][reader.type]['dataformat_id']))
                        major_version = int(json.dumps(qi['metadata'][reader.type]['major_version']))
                        minor_version = int(json.dumps(qi['metadata'][reader.type]['minor_version']))
                        self.assets.append(assetInfo(ta, filesize, b, np, srs
                                                    , compressed
                                                    , copc
                                                    , creation_doy
                                                    , creation_year
                                                    , point_record_format
                                                    , major_version
                                                    , minor_version))
                    except:
                        self.assets.append(assetInfo(ta, filesize, b, np, srs))

                else:
                    raise ValueError(f"{self.assettype} type not supported!")
                
                # if len(srs) == 0:
                #     raise Exception(f"Asset {ta} does not have srs")
                
            self.assetcount = len(self.assets)

            self.overallbounds = self.update_overall_bounds()
            if len(self.assets) > 0:
                self.srs = self.assets[0].srs
            else:
                self.srs = ""
            self.__sum_points()
            self.__sum_sizes()


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

        DEPRECATED...do not use
        
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
                # get file size
                filesize = self.__get_asset_size(ta)

                # use PDAL python bindings to find the srs of our data...look at first asset
                if self.assettype == 'points':
                    reader = pdal.Reader(ta)
                    p = reader.pipeline()
                    qi = p.quickinfo[reader.type]
                    #print(json.dumps(qi, indent = 4))
                    try:
                        srs = json.dumps(qi['srs']['json'])
                    except:
                        srs = ""

                    # deal with empty json for srs
                    if srs == "{}": srs = ""

                    b = Bounds.from_string((json.dumps(qi['bounds'])))
                    np = int(json.dumps(qi['num_points']))
                else:
                    raise ValueError(f"{self.assettype} type not supported!")
                
                # if len(srs) == 0:
                #     raise Exception(f"Asset {ta} does not have srs")
                
                self.assets.append(assetInfo(ta, filesize, b, np, srs))

            self.overallbounds = self.update_overall_bounds()
            self.srs = self.assets[0].srs
            self.__sum_points()
            self.__sum_sizes()

            if (self.testsrs):
                self.srsmatch = self.__test_assets_srs(testtype = self.testtype)
        elif len(tassets):
            for ta in tassets:
                self.assets.append(assetInfo(ta, filesize = -1, bounds = None, numpoints = 0, srs = ""))

            self.overallbounds = None
            self.totalpoints = 0

        return len(self.assets)

    def __base_is_remote(self) -> bool:
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
        if self.__base_is_remote():
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
        if len(self.assets) > 0:
            for asset in self.assets:
                self.totalpoints = self.totalpoints + asset.numpoints

            return self.totalpoints
        
        return 0
    
    def __sum_sizes(self) -> int:
        self.assetsize = 0
        if len(self.assets) > 0:
            for asset in self.assets:
                self.assetsize = self.assetsize + asset.filesize

            # check for negative sum...indicates files sizes not available for assets
            if self.assetsize < 0:
                self.assetsize = -1

            return self.assetsize
        
        return -1
    
    def __get_asset_size(self, filename: str) -> int:
        """
        Retrieves the size of a remote file without downloading it or
        gets the size of a local file.

        Returns:
            int: The size of the file in bytes, or -1 if the size cannot be determined.
        """
        if 'http' in filename.lower():
            # remote file
            try:
                response = requests.head(filename)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                file_size = int(response.headers.get('content-length', -1))
            except requests.exceptions.RequestException as e:
                file_size = -1
        else:
            # local file
            try:
                file_size = os.path.getsize(filename)
            except:
                file_size = -1
        
        return file_size
