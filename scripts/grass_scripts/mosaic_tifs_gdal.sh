# create log file
BAND=dem
LOGPATH=/mnt/poseidon/remotesensing/arctic/scripts/grass_scripts/logs/${BAND}_mosaic.log
exec 2> >(tee $LOGPATH)

# set parameters
TIF_DIR=/mnt/poseidon/remotesensing/arctic/data/rasters/arctic_dem
OUT_DIR=/mnt/poseidon/remotesensing/arctic/data/rasters/arctic_dem_gridded
MOSAIC_NAME=${BAND}_mosaic_ak_arctic
TIF_LIST=${BAND}_tif_list.txt
MAPSET=morgan
BASE_FILE=*_dem.tif
COLOR=elevation
SHAPEFILES=/mnt/poseidon/remotesensing/arctic/data/vectors/supplementary/tundra_alaska_grid_cells

# helper function
removeExtension() {
    file="$1"
    echo "$(echo "$file" | sed -r 's/\.[^\.]*$//')"
}

# use GDAL to create mosaic and clip
cd $TIF_DIR
ls -1 $BASE_FILE > $TIF_LIST # overwrites existing listr
gdal_merge.py --config GDAL_CACHEMAX 500 -v -o ${MOSAIC_NAME}.tif --optfile $TIF_LIST

# clip data but maintain entire raster extent (otherwise -crop_to_cutline)
# echo Cutting to polygons ...
# for F in ${SHAPEFILES}/*.shp; do
#     echo Working on "$(basename ${F})"
#     gdalwarp -cutline $F ${MOSAIC_NAME}.tif "${OUT_DIR}/$(removeExtension "$(basename ${F})")_${BAND}".tif
# done
# echo COMPLETE!

# import into GRASS
# grasscr global_latlon $MAPSET
# r.in.gdal --o -e input=${MOSAIC_NAME}.tif output=$MOSAIC_NAME
# r.colors map=$MOSAIC_NAME color=$COLOR