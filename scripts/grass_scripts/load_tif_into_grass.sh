MAPSET=morgan #mapset must already be created
BANDS=(B2 B3 B4 B8)
MONTHS=("01" "02" "03" "04" "05")
INPATH=/mnt/poseidon/remotesensing/arctic/data/rasters/S2SR/ak_arctic
START=0 #first tile of interest
END=0 #last tile of interest

grasscr global_latlon ${MAPSET}


# loop through band folders
for b in "${BANDS[@]}"
do

  band_folder=${INPATH}/${b}

  # loop through month folders
  for m in "${MONTHS[@]}"
  do

    month_folder=`find ${band_folder}/ -type d -name 2019-${m}*`

    # loop through GRIDCELL tifs
    for(( i=$START; i<=$END; i++ ))
    do

      tif_file=${month_folder}/GRIDCELL_${i}.tif
      filename=$(basename -- "$tif_file")
      name="${filename%.*}"
      output_name=${name}_2019-${m}_${b}

      # echo ${tif_file}
      # echo ${output_name}

      # import tif
      r.in.gdal -e input=${tif_file} output=${output_name}
      r.colors map=${output_name} color=grey

    done

  done

done