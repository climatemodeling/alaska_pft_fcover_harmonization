path=/mnt/poseidon/remotesensing/arctic/data/rasters/model_results_tiled
cd ${path}_01
for file in ${path}/*.tif
do 
    # adjust NS resolution 
    base="$(basename "$file" .tif)"
    base="${base// /_}"
    gdalwarp -overwrite -t_srs EPSG:4326 "${file}" "${base}.tif"
done