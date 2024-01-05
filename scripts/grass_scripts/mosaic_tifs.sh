#!/bin/bash

# set parameters
BANDS=(B5)
OUT_PREFIX="ALL_GRIDCELLS_MOSAIC" #beginning of output mosaic name
IN_SUFFIX="*_$BANDS" #ending of input raster name in grass
MAPSET=morgan
NUM_IMGS=4595

grasscr global_latlon ${MAPSET}

# loop through band names
for b in "${BANDS[@]}"; do

  # create mosaic basename
  outname="${OUT_PREFIX}_${b}"

  # avoid memory issues
  i=0
  while [ $i -lt 4595 ]; do

    # first end-point = 500
    if [ $i -eq 0 ]; then
      ending=500
    fi
    # last end-point: don't go above maximum
    if [ $ending -ge 4595 ]; then
      ending=4595
    fi

    echo STARTING AT $i AND ENDING AT $ending
    # build array of grass map names
    array=()
    while [ $i -lt $ending ]; do
      array+=(GRIDCELL_${i}_2019-06_${b})
      i=$(( $i + 1 ))
    done

    # create list of map names
    printf -v joined '%s,' "${array[@]}"
    export MAPS="${joined%,}"
    g.region rast=$MAPS
    r.patch in=$MAPS out=${outname}_${ending}
    r.colors map=${outname}_${ending} color=grey

    # update end point to i + 500
    ending=$(( $i + 500 ))

  done

done