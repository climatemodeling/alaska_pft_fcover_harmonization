#!/usr/bin/python3
# conda activate gee
# python S2_timeseries_merge.py

import os
import pandas as pd
import glob

# set input vars
UID = 'UID'
DIR = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/med_band_2019/med_band_data'
DIR2 = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03'

# set output vars
OUT = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/med_band_2019'
NAME = 'ALL'
BAND = 'B2_median'

##########################################################################################
# Create band.csv of entire year
##########################################################################################

# loop through folders in dir and append dataframes
dfs = []
folders = sorted(glob.glob(f'{DIR}/*'))

# loop through band folders
for folder in folders:
    
    band = os.path.basename(folder) + '_median'
    
    # loop through phenology files
    files = sorted(glob.glob(f'{folder}/*.csv'))
    ds = []
    uids = []
    
    # loop through dates
    for f in files:
        
        # read data for date
        csv = pd.read_csv(f, index_col=['Unnamed: 0'])
        cols = csv.columns.to_list()
        
        # if missing date/band data"
        if 'date_first' in cols:
            if csv['date_first'].isnull().values.all():
                csv[band] = np.nan
                csv.drop(columns=['date_first'], inplace=True)
            else:
                csv.drop(columns=['date_first'], inplace=True)
        else:
            csv[band] = np.nan
        ds.append(csv[band])
        uids.append(csv[UID])
    
    # create dataframe
    dates = [os.path.basename(x).split('_')[0] for x in files]
    df = pd.concat(ds, axis=1)
    df.columns = pd.to_datetime(dates)
    df.index = uids[0]
    df.to_csv(f'{OUT}/{NAME}_{band}_{DATE}.csv')
    
##########################################################################################
# Extract source information
##########################################################################################
    
# get source info
files = [f'{DIR2}/AKAVA_pft_fcover_01.csv', 
         f'{DIR2}/AKVEG_pft_fcover_00.csv', 
         f'{DIR2}/NEON_pft_fcover_00.csv']
dfs = []
for file in files:
    df = pd.read_csv(file)
    dfs.append(df)
    
dfs[0]['source'] = 'AKAVA'
dfs[0].set_index('Releve number', inplace=True)
dfs[1]['source'] = 'AKVEG'
dfs[1].set_index('Site Code', inplace=True)
dfs[2]['source'] = 'NEON'
dfs[2].set_index('name', inplace=True)

sources = []
for df in dfs:
    source = df['source']
    sources.append(source)

for file, df in zip(files, dfs):
    df.to_csv(file)

##########################################################################################
# Add source column to each band.csv
########################################################################################## 
    
# merge info
concated = []
band_files = sorted(glob.glob(f'{OUT}/*.csv'))

dfs = []
for f in files:
    df = pd.read_csv(f, index_col=0)
    dfs.append(df)
sourcedata = pd.concat(dfs)
sourcedata.index = sourcedata.index.map(str)

# loop through 2019 bands and overwrite
for bfile in band_files:
    
    b_df = pd.read_csv(bfile, index_col=0)
    b_df.index = b_df.index.map(str)
    band = os.path.basename(bfile)
    
    conc = pd.concat([b_df, sourcedata], axis=1, join='inner')
    source = conc['source']
    dates = conc.iloc[:,:12]
    fcover_match = pd.concat([dates, source], axis=1)
    fcover_match.to_csv(bfile)
