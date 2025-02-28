# code to trim FUSION rasters to match extent and CRS of SilviMetric rasters
#
# FUSION adds extra rows and columns around the perimeter of the area.
#
library(terra)
library(sf)
library(mapview)

HAGVRTfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_vrt_tifs/"
FUSIONfolder <- "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/Metrics_30METERS/"
SMfile <- "m_Z_max.tif"
FUSIONfile <- "elev_max_2plus_30METERS.img"

# read raster layers
HAGVRTrast <- rast(paste0(HAGVRTfolder, SMfile))
FUSIONrast <- rast(paste0(FUSIONfolder, FUSIONfile))

cat("FUSION output:    ncol=", ncol(FUSIONrast), "   nrow=",nrow(FUSIONrast), "   cells=", ncol(FUSIONrast) * nrow(FUSIONrast))
cat("HAGVRT output:    ncol=", ncol(HAGVRTrast), "   nrow=",nrow(HAGVRTrast), "   cells=", ncol(HAGVRTrast) * nrow(HAGVRTrast))

cat("FUSION raster:    ", summary(FUSIONrast))
cat("HAGVRT raster:    ", summary(HAGVRTrast))

# trim rasters...hopefully they match afterwards
tHAGVRTrast <- trim(HAGVRTrast)
tFUSIONrast <- trim(FUSIONrast)
ext(tFUSIONrast)
ext(tHAGVRTrast)

# FUSION outputs have simpler crs based on ESRI projection file
crs(tFUSIONrast) <- crs(tHAGVRTrast)
