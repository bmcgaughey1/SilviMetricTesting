# testing silvimetric...comparing outputs to FUSION and various HAG methods
#
library(terra)
library(sf)
library(mapview)

HAGVRTfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_vrt_tifs/"
HAGNNfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_nn_tifs/"
HAGFUSIONfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_normalized_tifs/"
FUSIONfolder <- "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/Metrics_30METERS/"
SMfile <- "m_Z_max.tif"
#SMfile <- "m_Z_mean.tif"
FUSIONfile <- "elev_max_2plus_30METERS.img"

# read raster layers
HAGVRTrast <- rast(paste0(HAGVRTfolder, SMfile))
HAGNNrast <- rast(paste0(HAGNNfolder, SMfile))
HAGFUSIONrast <- rast(paste0(HAGFUSIONfolder, SMfile))
FUSIONrast <- rast(paste0(FUSIONfolder, FUSIONfile))

cat("FUSION output:    ncol=", ncol(FUSIONrast), "   nrow=",nrow(FUSIONrast), "   cells=", ncol(FUSIONrast) * nrow(FUSIONrast))
cat("HAGVRT output:    ncol=", ncol(HAGVRTrast), "   nrow=",nrow(HAGVRTrast), "   cells=", ncol(HAGVRTrast) * nrow(HAGVRTrast))
cat("HAGNN output:     ncol=", ncol(HAGNNrast), "   nrow=",nrow(HAGNNrast), "   cells=", ncol(HAGNNrast) * nrow(HAGNNrast))
cat("HAGFUSION output: ncol=", ncol(HAGFUSIONrast), "   nrow=",nrow(HAGFUSIONrast), "   cells=", ncol(HAGFUSIONrast) * nrow(HAGFUSIONrast))

cat("FUSION raster:    ", summary(FUSIONrast))
cat("HAGVRT raster:    ", summary(HAGVRTrast))
cat("HAGNN raster:     ", summary(HAGNNrast))
cat("HAGFUSION raster: ", summary(HAGFUSIONrast))

# trim rasters...hopefully they match afterwards
tHAGVRTrast <- trim(HAGVRTrast)
tHAGNNrast <- trim(HAGNNrast)   # different min Y
tHAGNNrast <- crop(tHAGNNrast, tHAGVRTrast)
tHAGFUSIONrast <- trim(HAGFUSIONrast)
tFUSIONrast <- trim(FUSIONrast)
ext(tFUSIONrast)
ext(tHAGVRTrast)
ext(tHAGNNrast)
ext(tHAGFUSIONrast)

# FUSION outputs have simpler crs based on ESRI projection file
crs(tFUSIONrast) <- crs(tHAGFUSIONrast)

r <- tFUSIONrast - tHAGVRTrast
summary(r)
hist(r, nclass= 100)
r <- tFUSIONrast - tHAGNNrast
summary(r)
hist(r, nclass= 100)
r <- tFUSIONrast - tHAGFUSIONrast
summary(r)
hist(r, nclass= 100)
