# testing silvimetric...comparing outputs to FUSION and various HAG methods
#
library(terra)
library(sf)
library(mapview)
library(fusionwrapr)

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




# compare CHMs
# NOTE: need to use pixel-is-cell alignment in SilviMetric to align with FUSION CHM
CHMVRTfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_CHM_vrt_tifs/"
SMfile <- "m_Z_max.tif"
r <- rast(paste0(CHMVRTfolder, SMfile))
plot(r)
#summary(r)
#writeDTM(r, paste0(HAGVRTfolder, "m_Z_max.dtm"), xyunits = "M", zunits = "M", coordsys = 2, zone=10, horizdatum = 2, vertdatum = 2)

# didn't do a CHM in AP run
FUSIONfolder <- "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2025-02-04/FINAL_FUSIONTestMetrics_2025-02-04/CanopyHeight_1p5METERS/"
FUSIONfile <- "CHM_filled_not_smoothed_1p5METERS.img"
fr <- rast(paste0(FUSIONfolder, FUSIONfile))
plot(fr)
#summary(fr)
crs(fr) <- crs(r)

ext(r)
ext(fr)

tr <- crop(fr, r)
ext(tr)
diff <- tr - r
summary(diff)
plot(diff)










# canopy height model
folder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/Plumas_CHM_pic_vrt_tifs/"
file <- "m_Z_max.tif"
r <- rast(paste0(folder, file))
plot(r)

writeDTM(r, "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/Plumas_CHM_pic_vrt_tifs/CHM.dtm"
         , xyunits = "M"
         , zunits = "M"
         , coordsys = 1
         , zone = 10
         , horizdatum = 2
         , vertdatum = 2)
