#!/bin/bash
# enable next line for debugging purpose
# set -x

LOGPATH=/mnt/poseidon/remotesensing/arctic/scripts/snap_esa/S1_Coversion_0111242117.log
exec 2> >(tee $LOGPATH)

############################################
# Script Information
############################################

# source: https://senbox.atlassian.net/wiki/spaces/SNAP/pages/70503475/Bulk+Processing+with+GPT
# usage: loops through a dir of images and applies a "graph" (snap workflow .xml) to each image
# output: processed images in a specified directory

############################################
# User Configuration
############################################

# path to snap and gpt executable
export PATH=/home/6ru/snap/bin:$PATH
gptPath="gpt"

############################################
# Command line handling
############################################

# use third parameter for path to source products
sourceDirectory="$1"

# use fourth parameter for path to target products
targetDirectory="$2"

############################################
# Helper functions
############################################
removeExtension() {
    file="$1"
    echo "$(echo "$file" | sed -r 's/\.[^\.]*$//')"
}

############################################
# Main processing
############################################

# Create the target directory if needed
if ! test -d ${targetDirectory}; then
  mkdir -p "${targetDirectory}"
fi

# the d option limits the elements to loop over directories. Remove d (ls -1d) if you want to use files
for F in $(ls -1 "${sourceDirectory}"/*.dim); do

  # create target file name
  sourceFile="$(realpath "$F")"
  targetFile="${targetDirectory}/$(removeExtension "$(basename ${F})")".tif
  
  # do not overwrite existing files
  if [ -d ${targetFile} ]; then
    echo "File ${targetFile} already exists."
  else
    echo "Working on ${sourceFile} ..."
    # gpt <op>/<graph-file> <source-file-i> plus whatever flags
    # gpt <op> -h for how to structure command flags
    ${gptPath} Write -Ssource=${sourceFile} -Pfile=${targetFile} -PformatName=GeoTIFF-BigTIFF
    echo SUCCESS! Written to ${targetFile}
  fi

done
