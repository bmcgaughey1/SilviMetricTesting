# code to trim FUSION rasters to match extent and CRS of SilviMetric rasters
#
# FUSION adds extra rows and columns around the perimeter of the area.
#
library(terra)
library(sf)
library(mapview)
library(stringr)

HAGVRTfolder <- "C:/Users/bmcgaughey/SilviMetricTesting/TestOutput/plumas_vrt_tifs/"
FUSIONfolder <- "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/Metrics_30METERS/"
SMfile <- "m_Z_max.tif"
FUSIONfile <- "elev_max_2plus_30METERS.img"
FUSIONfiles <- list.files(path = FUSIONfolder, pattern = "\\.img$")
                                   
# read raster layer from SM...this will be used to set the extent
HAGVRTrast <- rast(paste0(HAGVRTfolder, SMfile))

# loop through FUSION outputs and crop to match SM output
# loop isn't the "best" way to do this but it works
for (f in FUSIONfiles) {
  FUSIONrast <- rast(paste0(FUSIONfolder, f))
  tFUSIONrast <- crop(FUSIONrast, HAGVRTrast)

  crs(tFUSIONrast) <- crs(HAGVRTrast)
  writeRaster(tFUSIONrast, filename = paste0(FUSIONfolder, "TRIMMED/", str_replace(basename(f), ".img", ".tif")), overwrite = T)
}

# check a file
FUSIONfolder <- "H:/FUSIONTestMetrics/Products_FUSIONTestMetrics_2024-05-16/FINAL_FUSIONTestMetrics_2024-05-16/Metrics_30METERS/TRIMMED/"
FUSIONfile <- "elev_max_2plus_30METERS.tif"

HAGVRTrast <- rast(paste0(HAGVRTfolder, SMfile))
FUSIONrast <- rast(paste0(FUSIONfolder, FUSIONfile))

cat("FUSION output:    ncol=", ncol(FUSIONrast), "   nrow=",nrow(FUSIONrast), "   cells=", ncol(FUSIONrast) * nrow(FUSIONrast))
cat("HAGVRT output:    ncol=", ncol(HAGVRTrast), "   nrow=",nrow(HAGVRTrast), "   cells=", ncol(HAGVRTrast) * nrow(HAGVRTrast))
ext(FUSIONrast)
ext(HAGVRTrast)

crs(HAGVRTrast)
crs(FUSIONrast)

plot(HAGVRTrast, colNA = 'red')
plot(FUSIONrast, colNA = 'red')
