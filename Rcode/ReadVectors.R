# test files produced by assetCatalog
library(terra)
library(mapview)
library(sf)

folder <- "C:/Users/bmcgaughey/SilviMetricTesting/Python/"
file <- "assets.gpkg"
file <- "COL_2007_RiverCorridor_LakeRoosevelt__LAS.gpkg"

#folder <- "G:/R_Stuff/PlotClipping/KyPtCloudTileIndex/"
#file <- "Kentucky_5k_PointCloudGrid.shp"

ov <- st_read(paste0(folder, file), layer = "overall")
v <- st_read(paste0(folder, file), layer = "assets")
#plot(v)

# set projection...missing in data
# WA State Plane North...2285 for sherman pass
st_crs(ov) <- 26911
st_crs(v) <- 26911

# this won't show anything in RStudio viewer. Export to HTML to see map.
mapview(list(ov, v))
