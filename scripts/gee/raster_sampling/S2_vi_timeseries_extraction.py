#!/usr/bin/python3
# conda activate gee
# mpiexec -n 3 python3 S2_timeseries_extraction.py

import ee
ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

import geemap
import os
import time
from datetime import date, timedelta
import pandas as pd
import numpy as np
from mpi4py import MPI
import json
import pprint

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

##########################################################################################
# Set Parameters
##########################################################################################

# cloud filter params
CLOUD_FILTER = 90

# area of interest params
GEOJSON_PATH = ''
COUNTRY = ''
STATE = 'AK' # abbreviated for HUC watershed
POINTBUFFER = 30 # meters
ROI = 'HUC' # STATE, COUNTRY, BBOX, or HUC
HUCLIST = ['190604', '190603', '190602'] # must be list
DIR_PATH = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/med_band_2019/med_band_data_summer'
if not os.path.isdir(DIR_PATH):
    os.mkdir(DIR_PATH)

# data Information
IDCOL = 'UID'
SCALE = 10
BANDS = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8A', 'B9', 'B11', 'B12']
start_date = date(2019, 6, 1)# Year-Month-Day
end_date = date(2019, 8, 31) # Year-Month-Day minus 5 for even intervals (6 days for 2020)
TIMESTEP = None # months, days, or None
DAYS = '' # if TIMESTEP = days
MONTHS = '' # if TIMESTEP = months

##########################################################################################
# Create ee_to_df function for exporting
##########################################################################################

def ee_to_df(ee_object, col_names, sort_columns=False):
    if isinstance(ee_object, ee.Feature):
        ee_object = ee.FeatureCollection([ee_object])

    if not isinstance(ee_object, ee.FeatureCollection):
        raise TypeError("ee_object must be an ee.FeatureCollection")

    try:
        property_names = ee_object.first().propertyNames().sort().getInfo()
        #data = ee_object.map(lambda f: ee.Feature(None, f.toDictionary(property_names)))
        data = ee_object
        data = [x["properties"] for x in data.getInfo()["features"]]
        df = pd.DataFrame(data)

        if col_names is None:
            col_names = property_names
            col_names.remove("system:index")
        elif not isinstance(col_names, list):
            raise TypeError("col_names must be a list")

        df = df[col_names]

        if sort_columns:
            df = df.reindex(sorted(df.columns), axis=1)

        return df
    
    except Exception as e:
        raise Exception(e)

##########################################################################################
# Set Date Ranges
##########################################################################################

# days

if TIMESTEP == 'days':

    def create_list_of_dates(start_date, end_date):
        dates = []
        delta = end_date - start_date   # returns timedelta

        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            dates.append(day)
        return dates

    def create_time_intervals(dates_list, Interval):
        time_df = pd.DataFrame({'Date': dates_list}).astype('datetime64[ns]')
        interval = timedelta(Interval)
        grouped_cr = time_df.groupby(pd.Grouper(key='Date', freq=interval))
        date_ranges = []
        for i in grouped_cr:
            date_ranges.append(((str(i[1].min()[0]).split(' ')[0]), 
                                (str(i[1].max()[0]).split(' ')[0])))
        return date_ranges

    date_ranges = create_time_intervals(create_list_of_dates(start_date, 
                                                             end_date), 
                                        DAYS)
    print(date_ranges)

##########################################################################################
# months

elif TIMESTEP == 'months':

    def create_list_of_dates(start_date, end_date):

        dates = []
        end_date = end_date - relativedelta(months=MONTHS-1)
        for i in range(MONTHS):
            delta = relativedelta(months=i)
            month_start = start_date + delta
            month_end = end_date + delta
            dates.append((month_start.strftime('%Y-%m-%d'), 
                          month_end.strftime('%Y-%m-%d')))
        return dates

    date_ranges = create_list_of_dates(start_date, end_date)
    print(date_ranges)

##########################################################################################
# no step

elif TIMESTEP == None:

    date_ranges = [(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))]

else:

    print("Invalid TIMESTEP selection. Use 'days', 'months', or None.")

##########################################################################################
# Set Ranks by Number of HUCS
##########################################################################################

# create huc (rank) directories
allhucs = np.array(HUCLIST)
allhucs = np.array_split(allhucs, size) # split array into x pieces
for r in range(len(allhucs)):
    if r == rank:
        CURRENTROI = HUCLIST[r] # select object from list
        PATH = f'{DIR_PATH}/{CURRENTROI}'
    else:
        pass

print('RANK:', rank, CURRENTROI, flush = True)

# create timestamp directories within each huc (rank)
if os.path.isdir(PATH):
    pass
else:
    os.mkdir(PATH)

##########################################################################################
# Set GEE Vector Bounds
##########################################################################################

# Import admin data and select country to create grid around
if ROI == 'STATE':
    grid_location_ee = (ee.FeatureCollection("FAO/GAUL/2015/level1")
                        .filterMetadata('ADM0_NAME', 'equals', COUNTRY)
                        .filterMetadata('ADM1_NAME', 'equals', STATE))

elif ROI == 'COUNTRY':
    grid_location_ee = (ee.FeatureCollection("FAO/GAUL/2015/level1")
                        .filterMetadata('ADM0_NAME', 'equals', COUNTRY))
	
elif ROI == 'BBOX':
	grid_location_ee = geemap.geojson_to_ee(GEOJSON_PATH)
    
elif ROI == 'HUC':
    grid_location_ee = (ee.FeatureCollection("USGS/WBD/2017/HUC06")
                        .filter(ee.Filter.inList('huc6', [CURRENTROI])))
    
else:
    print('Invalid region of interest. Choose STATE, COUNTRY, or HUC')
    quit

##########################################################################################
# Get Sampling Points
##########################################################################################

##########################################################################################
# AK-AVA Turboveg post-2000

# load observations
di = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/'
fi = 'AKAVA_pft_fcover_01.csv'
obs_data = pd.read_csv(di + fi)

# extract geometry and unique ID
ava_geom = obs_data[['Latitude (decimal degrees)', 
                     'Longitude (decimal degrees)', 
                     'Releve number']]

ava_geom.columns = ['Lat', 'Lon', 'UID']

##########################################################################################
# AKVEG North Slope

# load observations
di = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/'
fi = 'AKVEG_pft_fcover_00.csv'
obs_data = pd.read_csv(di + fi)

# extract geometry and unique ID
akv_geom = obs_data[['Latitude', 
                     'Longitude', 
                     'Site Code']]

akv_geom.columns = ['Lat', 'Lon', 'UID']

##########################################################################################
# NEON North Slope

# load observations
di = '/mnt/poseidon/remotesensing/arctic/data/training/Test_03/'
fi = 'NEON_pft_fcover_00.csv'
obs_data = pd.read_csv(di + fi)

# extract geometry and unique ID
neo_geom = obs_data[['subplot_lat', 
                     'subplot_lon', 
                     'name']]

neo_geom.columns = ['Lat', 'Lon', 'UID']

##########################################################################################
# Unmixing North Slope data

# load observations
di = '/mnt/poseidon/remotesensing/arctic/data/training/testData_unmixingRegression/'
fi = 'purePFT_merged_fCover_Macander2017_geometry.geojson'
#fi = 'randomPts_fCover_10kmDist_Macander2017_geometry.geojson'

with open(di + fi) as file:
    data = json.load(file)

geom = []
for feature in data['features']:
    lon = feature['properties']['xcoord']
    lat = feature['properties']['ycoord']
    uid = feature['properties']['id']
    if lon is not None:
        geom.append([lon, lat, uid])

# extract geometry and unique ID
obs_geom = pd.DataFrame(geom, columns=['xcoord', 'ycoord', 'id'])

# create ee object (feature collection)
obs_points = geemap.df_to_ee(obs_geom, 
                             latitude='ycoord', 
                             longitude='xcoord')

##########################################################################################
# combine
obs_geom = pd.concat([ava_geom, akv_geom, neo_geom], 
                     axis=0, 
                     ignore_index=True)

# create ee object (feature collection)
obs_points = geemap.df_to_ee(obs_geom, 
                             latitude='Lat', 
                             longitude='Lon')

##########################################################################################

# select points that intercept HUC
samplepoints = obs_points.filterBounds(grid_location_ee)

# create dictionary of grid coordinates
points_dict = samplepoints.getInfo()
feats = points_dict['features']

# get ID column
unique_ids = []
for f in feats:
    id = f['properties'][IDCOL]
    unique_ids.append(id)

# Create a list of several ee.Geometry.Polygons
points = []
for f in feats:
    coords = f['geometry']['coordinates']
    point = ee.Geometry.Point(coords)
    # create buffer around point for later reduce regions
    buffered = point.buffer(POINTBUFFER)
    points.append(buffered)

# Make a feature collection for export purposes
points_ee = ee.FeatureCollection(points)
print(f'{len(points)} {POINTBUFFER}-meter buffered points within HUC6 {CURRENTROI}.')

##########################################################################################
# Set Cloud Mask Function for S2-SR
##########################################################################################

def mask_s2_clouds(image):
    # select bitmask band
    qa = image.select('QA60')

    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11

    # Both flags should be set to zero, indicating clear conditions.
    mask = (qa.bitwiseAnd(cloud_bit_mask).eq(0).And
            (qa.bitwiseAnd(cirrus_bit_mask).eq(0)))

    return image.updateMask(mask)

def add_variables(image):
    # Compute time in fractional years since the epoch.
    date = ee.Date(image.get('system:time_start')).millis()
    # Return the image with the added bands.
    return (image
            .addBands(image.normalizedDifference(['B8', 'B4']).rename('ndvi'))
            .addBands(ee.Image(date).rename('date').float())
           )


##########################################################################################
# Apply Cloud Mask, NDVI, and Date Functions
##########################################################################################
s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterBounds(samplepoints)
                .filterDate(str(start_date), str(end_date))
                #.filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER))
            )

s2_sr = (s2_sr_col.map(mask_s2_clouds).map(add_variables)).select(['ndvi', 'date'])

print("Cloud and shadow mask applied to tiles. NDVI and Date bands created.")

##########################################################################################
# Sample Raster and Export Table
##########################################################################################

start = time.time()

# Loop through date ranges and export sampled composites
for RANGE in date_ranges:
    
    # select cloud-filtered sentinel 2 imagery for time step
    s2_by_date = s2_sr.filterDate(RANGE[0], RANGE[1])
    sentinel2 = s2_by_date.filterBounds(points_ee)

    # set band list
    b_list = ['ndvi_median', 'date_first']

    # if image collection has images:
    if sentinel2.size().getInfo() != 0:

        # create composite for time step
        composite = (sentinel2.select('ndvi')).reduce(ee.Reducer.median())
        composite_date = (sentinel2.select('date')).reduce(ee.Reducer.first())
        composite = composite.addBands(composite_date)

        # sample composite with points (returns feature collection)
        sampled = composite.reduceRegions(collection = points_ee,
                                          reducer = ee.Reducer.median(),
                                          scale = SCALE,
                                          crs = 'EPSG:4326')
        
        # export feature collection as csv
        FILE = f'{PATH}/{RANGE[0]}_to_{RANGE[1]}.csv'
        if os.path.isfile(FILE):
            print('FILE:', FILE, ' ALREADY EXISTS', flush=True)
        else:

            # export to dataframe
            df = ee_to_df(sampled, col_names=b_list)
            df[IDCOL] = unique_ids
            df.to_csv(FILE)
            print(f'Saved to {FILE}')

    # if image collection doesn't have images:
    else:
        
        FILE = f'{PATH}/{RANGE[0]}_to_{RANGE[1]}.csv'
        if os.path.isfile(FILE):
            print('FILE:', FILE, ' ALREADY EXISTS', flush=True)
        else:
            # create dataframe from original point data
            df = pd.DataFrame(unique_ids, columns=[IDCOL])
            df.loc[:, b_list] = np.nan
            df.to_csv(FILE)
            print(f'No images in collection. Saved dummy data to {FILE}')

stop = time.time()
time_taken = (stop - start)/3600
print('PROCESS COMPLETE! TOOK', round(time_taken, 3), 'HOURS')