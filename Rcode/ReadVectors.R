# test files produced by assetCatalog
library(terra)
library(mapview)
library(sf)

folder <- "C:/Users/bmcgaughey/SilviMetricTesting/Python/"
file <- "assets.gpkg"

#folder <- "G:/R_Stuff/PlotClipping/KyPtCloudTileIndex/"
#file <- "Kentucky_5k_PointCloudGrid.shp"

v <- st_read(paste0(folder, file))
#plot(v)

# this won't show anything in RStudio viewer. Export to HTML to see map.
mapview(v)
