# create log file
BAND=B8A
LOGPATH=/mnt/poseidon/remotesensing/arctic/scripts/grass_scripts/logs/${BAND}_mosaic_summer_2019.log
exec 2> >(tee $LOGPATH)

# set parameters
TIF_DIR=/mnt/poseidon/remotesensing/arctic/data/rasters/S2SR/ak_arctic_summer/${BAND}/2019-06-01_to_2019-08-31
OUT_DIR=$TIF_DIR
MOSAIC_NAME=${BAND}_2019_summer_mosaic_ak_arctic
TIF_LIST=${BAND}_summer_2019_tif_list.txt
MAPSET=morgan

# use GDAL to create mosaic
cd $TIF_DIR
ls -1 *.tif > $TIF_LIST
gdal_merge.py -v -o ${MOSAIC_NAME}.tif --optfile $TIF_LIST

# import into GRASS
grasscr global_latlon ${MAPSET}
r.in.gdal -e input=${MOSAIC_NAME}.tif output=$MOSAIC_NAME
r.colors map=$MOSAIC_NAME color=grey