#!/bin/bash
# enable next line for debugging purpose
# set -x

LOGPATH=/mnt/poseidon/remotesensing/arctic/scripts/snap_esa/S1_Preprocessing_010524.log
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

# first parameter is a path to the graph xml
graphXmlPath="$1"

# second parameter is a path to a parameter file (.parameter) for graph parameters
parameterFilePath="$2"

# use third parameter for path to source products
sourceDirectory="$3"

# use fourth parameter for path to target products
targetDirectory="$4"

# the fifth parameter is a file prefix for the target product name, typically indicating the type of processing
# Orb_NR_Cal_TC
targetFilePrefix="$5"

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
for F in $(ls -1 "${sourceDirectory}"/*.zip); do

  # create target file name
  sourceFile="$(realpath "$F")"
  targetFile="${targetDirectory}/${targetFilePrefix}_$(removeExtension "$(basename ${F})").dim"
  
  # do not overwrite existing files
  if [ -f ${targetFile} ]; then
    echo "File ${targetFile} already exists."
  else
    echo "Working on ${sourceFile} ..."
    ${gptPath} ${graphXmlPath} -e -p ${parameterFilePath} -t ${targetFile} ${sourceFile}
    echo SUCCESS! Exported to ${targetFile}
  fi

done
