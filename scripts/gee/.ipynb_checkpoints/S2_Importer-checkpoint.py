#!/usr/bin/python3
# conda activate gee
# mpiexec -n 4 python3 S2_Importer.py

import ee
ee.Initialize()

import geemap
import os
import time
import sys
from datetime import date, timedelta
import pandas as pd
import numpy as np
from mpi4py import MPI
import requests

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

#country_name = sys.argv[1].lower()
#SITE = ' '.join([i.capitalize() for i in country_name.replace('_', ' ').split(' ')])

# cloud filter params
CLOUD_FILTER = 90
CLD_PRB_THRESH = 50
NIR_DRK_THRESH = 0.15
CLD_PRJ_DIST = 1
BUFFER = 50

# bounding box params
COUNTRY = ''
STATE = 'AK' #Abbreviated for WATERSHED
ROI = 'HUC6' #STATE, COUNTRY, BBOX, or HUC6
HUC = '190604' #Can be list
GEOJSON_PATH = ''
GRIDSIZE = 10000 #km*1000
DIR_PATH = '/mnt/poseidon/remotesensing/arctic/data/rasters/S2SR'

# data Information
SCALE = 10
BANDS = ['B2', 'B3', 'B4','B8']
start_date = date(2019, 1, 1) # Year-Month-Day (minus 5 days to make it an even 38, 30 day intervals)
end_date = date(2019, 12, 26) # Year-Month-Day
INCREMENT = 15 #days

####################################### Set Date Ranges #######################################

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
        date_ranges.append(((str(i[1].min()[0]).split(' ')[0]), (str(i[1].max()[0]).split(' ')[0])))
    return date_ranges

date_ranges = create_time_intervals(create_list_of_dates(start_date, end_date), INCREMENT)

###################################### Set Ranks by Watershed #####################################

all_bands = np.array(BANDS)
all_bands = np.array_split(all_bands, size) # split bands into "size" number of groups
for b in range(len(all_bands)):
    if b == rank:
        bands_list = all_bands[b] # select one group of bands from list
        rankname = ''.join(bands_list)
        DIR_PATH = f'{DIR_PATH}/HUC_{HUC}/{rankname}_{b}'
    else:
        pass

print('RANK:', rank, bands_list, flush = True)

for RANGE in date_ranges:
    if os.path.isdir(f'{DIR_PATH}/{RANGE[0]}_to_{RANGE[1]}'):
        pass
    else:
        os.makedirs(f'{DIR_PATH}/{RANGE[0]}_to_{RANGE[1]}')

###################################### Set Location ###############################################

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
    
elif ROI == 'HUC6':
    grid_location_ee = (ee.FeatureCollection("USGS/WBD/2017/HUC06")
                        .filterMetadata('huc6', 'equals', HUC))
    
else:
    print('Invalid region of interest. Check STATE, COUNTRY, HUC')
    quit
		
######################################## Create Grid and Polygons #########################################

# Create grid
# https://developers.google.com/earth-engine/tutorials/community/drawing-tools

grid = grid_location_ee.geometry().coveringGrid(proj='EPSG:4326', scale=GRIDSIZE)

# Create dictionary of grid coordinates
grid_dict = grid.getInfo()
feats = grid_dict['features']

# Create a list of several ee.Geometry.Polygons
polys = []
for d in feats:
    coords = d['geometry']['coordinates']
    poly = ee.Geometry.Polygon(coords)
    polys.append(poly)

num_km = GRIDSIZE / 1000
print(f'{len(polys)} {num_km}-km grid cells superimposed on {HUC}.')

######################################## Sentinel-2 SR Functions Cloud Masking ##############################

def get_s2_sr_cld_col(aoi, start_date, end_date):
    # Import and filter S2 SR.
    s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER)))

    # Import and filter s2cloudless.
    s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
        .filterBounds(aoi)
        .filterDate(start_date, end_date))

    # Join the filtered s2cloudless collection to the SR collection by the 'system:index' property.
    return ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(
        primary = s2_sr_col,
        secondary = s2_cloudless_col,
        condition = ee.Filter.equals(
            leftField = 'system:index',
            rightField = 'system:index')
    ))

def add_cloud_bands(img):
    # Get s2cloudless image, subset the probability band.
    cld_prb = ee.Image(img.get('s2cloudless')).select('probability')

    # Condition s2cloudless by the probability threshold value.
    is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')

    # Add the cloud probability layer and cloud mask as image bands.
    return img.addBands(ee.Image([cld_prb, is_cloud]))


def add_shadow_bands(img):
    # Identify water pixels from the SCL band.
    not_water = img.select('SCL').neq(6)

    # Identify dark NIR pixels that are not water (potential cloud shadow pixels).
    SR_BAND_SCALE = 1e4
    dark_pixels = img.select('B8').lt(NIR_DRK_THRESH*SR_BAND_SCALE).multiply(not_water).rename('dark_pixels')

    # Determine the direction to project cloud shadow from clouds (assumes UTM projection).
    shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')));

    # Project shadows from clouds for the distance specified by the CLD_PRJ_DIST input.
    cld_proj = (img.select('clouds').directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST*10)
        .reproject(**{'crs': img.select(0).projection(), 'scale': 100})
        .select('distance')
        .mask()
        .rename('cloud_transform'))

    # Identify the intersection of dark pixels with cloud shadow projection.
    shadows = cld_proj.multiply(dark_pixels).rename('shadows')

    # Add dark pixels, cloud projection, and identified shadows as image bands.
    return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))


def add_cld_shdw_mask(img):
    # Add cloud component bands.
    img_cloud = add_cloud_bands(img)

    # Add cloud shadow component bands.
    img_cloud_shadow = add_shadow_bands(img_cloud)

    # Combine cloud and shadow mask, set cloud and shadow as value 1, else 0.
    is_cld_shdw = img_cloud_shadow.select('clouds').add(img_cloud_shadow.select('shadows')).gt(0)

    # Remove small cloud-shadow patches and dilate remaining pixels by BUFFER input.
    # 20 m scale is for speed, and assumes clouds don't require 10 m precision.
    is_cld_shdw = (is_cld_shdw.focalMin(2).focalMax(BUFFER*2/20)
        .reproject(**{'crs': img.select([0]).projection(), 'scale': 20})
        .rename('cloudmask'))

    # Add the final cloud-shadow mask to the image.
    return img_cloud_shadow.addBands(is_cld_shdw)


def apply_cld_shdw_mask(img):
    # Subset the cloudmask band and invert it so clouds/shadow are 0, else 1.
    not_cld_shdw = img.select('cloudmask').Not()

    # Subset reflectance bands and update their masks, return the result.
    #return img.select('B*').updateMask(not_cld_shdw)
    return img.updateMask(not_cld_shdw).select(BANDS)


############################ Apply the Cloud Mask #########################################

s2_sr_cld_col = get_s2_sr_cld_col(grid, str(start_date), str(end_date))
s2_sr = (s2_sr_cld_col
         .map(add_cld_shdw_mask)
         .map(apply_cld_shdw_mask)).select(list(bands_list))

print("Shadow mask applied to tiles.")
start = time.time()


######################################### Export #########################################

med_bands = [f'{b}_median' for b in bands_list] # .median() will add suffix to band

# Loop through date ranges and export composites
for RANGE in date_ranges:
    
    # select cloud-filtered sentinel 2 imagery for date range
    s2_by_date = s2_sr.filterDate(RANGE[0], RANGE[1])
    
    for location, poly in enumerate(polys):
        
        # create median composite for tile
        sentinel2 = s2_by_date.filterBounds(poly)
        composite = sentinel2.reduce(ee.Reducer.median())
        composite = composite.clip(poly)
        
        # export composite
        PATH = f'{DIR_PATH}/{RANGE[0]}_to_{RANGE[1]}/GRIDCELL_{location}.tif'
        if os.path.isfile(PATH):
            print('FILE:', PATH, ' ALREADY EXISTS', flush=True)
        else:
            try:
                # Multi-band GeoTIFF file.
                url = composite.getDownloadUrl({'bands': med_bands,
                                                'region': poly,
                                                'scale': 10,
                                                'format': 'GEO_TIFF',
                                                'filePerBand': False})
                print('Getting URL...')
                response = requests.get(url)
                with open(PATH, 'wb') as fd:
                    fd.write(response.content)
                    print(f'SUCCESS! Saved .tif at {PATH}')

            # if export fails create dummy tif with -999 band values
            except Exception as e:
                composite = ee.Image(-999).rename(med_bands[0])
                if len(med_bands) > 0:
                    for b in med_bands[1:]:
                        composite = composite.addBands(ee.Image(-999)).rename(med_bands[b])
                        
                try:
                    # Multi-band GeoTIFF file.
                    composite = composite.clip(poly)
                    url = composite.getDownloadUrl({'bands': med_bands,
                                                    'region': poly,
                                                    'scale': 10,
                                                    'format': 'GEO_TIFF',
                                                    'filePerBand': False})
                    print('Getting URL...')
                    response = requests.get(url)
                    with open(PATH, 'wb') as fd:
                        fd.write(response.content)
                        print(f'NO DATA. Saved dummy .tif at {PATH}')
                
                # if the dummy tif fails for some reason, just give up.
                except Exception as e:
                    print(e)
                    print('ERROR. Skipping...')

stop = time.time()
time_taken = (stop - start)/3600
print('PROCESS COMPLETE! TOOK', round(time_taken, 3), 'HOURS')