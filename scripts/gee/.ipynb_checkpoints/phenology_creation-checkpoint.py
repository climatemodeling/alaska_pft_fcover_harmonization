import ee
import geemap as gee
import geopandas as gpd
import pandas as pd
from shapely import wkt
from datetime import date, timedelta
import os
import numpy as np

ee.Initialize()

GRIDSIZE = 18000

##################################################################################
##################################################################################

# HUC data

# select HUCs of interest
hucs = ee.List(['190604', '190603', '190602'])
admin_fcol = (ee.FeatureCollection("USGS/WBD/2017/HUC06").filter(ee.Filter.inList('huc6', hucs)))
    
def make_grid(region, scale):
    """
    Creates a grid around a specified ROI.
    User inputs their reasonably small ROI.
    User inputs a scale where 100000 = 100km.
    """
    # Creates image with 2 bands ('longitude', 'latitude') in degrees
    lonLat = ee.Image.pixelLonLat()

    # Select bands, multiply times big number, and truncate
    lonGrid = (lonLat
               .select('latitude')
               .multiply(10000000)
               .toInt())
    latGrid = (lonLat
              .select('longitude')
              .multiply(10000000)
              .toInt())

    # Multiply lat and lon images and reduce to vectors
    grid = (lonGrid
            .multiply(latGrid)
            .reduceToVectors(
                geometry = region,
                scale = scale, # 100km-sized boxes = 100,000
                geometryType = 'polygon'))

    return(grid)

# Make your grid superimposed over ROI
grid_xkm = make_grid(admin_fcol, GRIDSIZE)

# Create dictionary of grid coordinates
grid_dict = grid_xkm.getInfo()
feats = grid_dict['features']

# Create a list of several ee.Geometry.Polygons
polys = []
for d in feats:
    coords = d['geometry']['coordinates']
    poly = ee.Geometry.Polygon(coords)
    polys.append(poly)

print("{} squares created.".format(len(polys)), flush=True)

# Make the whole grid a feature collection for export purposes
grid = ee.FeatureCollection(polys)
num_km = GRIDSIZE / 1000
print(f"{num_km} km squares.")

##################################################################################

# Observation Data

# load observation points for later
d = '/mnt/poseidon/remotesensing/arctic/data/vectors/AK-AVA_Turboveg/ak_tvexport_releves_header_data_for_vegbank_20181106_ALB.xlsx'
obs_data = pd.read_excel(d, skiprows=[1])
obs_data = obs_data.replace(-9, np.nan)

# extract geometry and future index
obs_geom = obs_data[['Latitude (decimal degrees)', 'Longitude (decimal degrees)', 'Releve number']]
obs_geom.set_index('Releve number', inplace=True)

# convert to ee feature collection object
to_ee(obs_geom, latitude='Latitude (decimal degrees)', longitude='Longitude (decimal degrees)')

##################################################################################
##################################################################################

# Set Vars

year1 = 2019
INCREMENT = 20 # timeseries composite length
startDate = str(year1) + '-03-15'
endDate = str(year1) + '-10-20'
start_date = date(2019, 3, 15)
end_date = date(2019, 10, 20)

# SET VI - ndvi, evi, ndpi, gcc
VI_index = 'ndvi' 
# Set the percentage of amplitude for the estimation of the threshold
th = 0.5 # advice 0.2-0.8
# Set the minimum NDVI value for the reclassification of non-vegetated
threshMin = 0.3
# Set scale of the analysis # between 100 to 50 optimal <50 to 10 no graph plot
scale = 1000

##################################################################################
##################################################################################

# General Functions

# Function that interpolates phenology curve
def cubicInterpolation(collection, step):

    listDekads = ee.List.sequence(1, collection.size().subtract(3), 1)

    def func1(ii):
        
        ii = ee.Number(ii)
        p0 = ee.Image(collection.toList(10000).get(ee.Number(ii).subtract(1)))
        p1 = ee.Image(collection.toList(10000).get(ii))
        p2 = ee.Image(collection.toList(10000).get(ee.Number(ii).add(1)))
        p3 = ee.Image(collection.toList(10000).get(ee.Number(ii).add(2)))

        diff01 = ee.Date(p1.get('system:time_start')).difference(ee.Date(p0.get('system:time_start')), 'day')
        diff12 = ee.Date(p2.get('system:time_start')).difference(ee.Date(p1.get('system:time_start')), 'day')
        diff23 = ee.Date(p3.get('system:time_start')).difference(ee.Date(p2.get('system:time_start')), 'day')
        diff01nor = diff01.divide(diff12)
        diff12nor = diff12.divide(diff12)
        diff23nor = diff23.divide(diff12)

        f0=p1
        f1=p2
        f0p = (p2.subtract(p0)).divide(diff01nor.add(diff12nor))
        f1p = (p3.subtract(p1)).divide(diff12nor.add(diff23nor))
        a = (f0.multiply(2)).subtract(f1.multiply(2)).add(f0p).add(f1p)
        b = (f0.multiply(-3)).add(f1.multiply(3)).subtract(f0p.multiply(2)).subtract(f1p)
        c=f0p
        d=f0

        xValues = ee.List.sequence(0,diff12.subtract(1),step)
        xDates = ee.List.sequence(p1.get('system:time_start'),p2.get('system:time_start'),86400000)
        
        def func2(x): 
            im = ee.Image(ee.Number(x).divide(diff12))
            return ((im.pow(3))
                    .multiply(a)
                    .add((im.pow(2)).multiply(b))
                    .add(im.multiply(c)).add(d)
                    .set('system:time_start', ee.Number(xDates.get(x))))
        
        interp = xValues.map(func2)
        return interp
        
    colInterp = listDekads.map(func1)
    colInterp = ee.ImageCollection(colInterp.flatten())
    return colInterp


# function that creates NDVI bands and rescales regular bands
def func_nfe(im):
    
    #RescaleBand
    blue = im.select('B2').multiply(0.0001)
    green = im.select('B3').multiply(0.0001)
    red = im.select('B4').multiply(0.0001)
    nir = im.select('B8').multiply(0.0001)
    swir = im.select('B12').multiply(0.0001)

    # Generate VIs
    ndvi = im.normalizedDifference(['B8','B4']).rename('ndvi')
    return (ndvi.where(ndvi.lt(threshMin), threshMin).set('system:time_start', im.get('system:time_start')))


# function that creates a list of dates
def create_list_of_dates(start_date, end_date):
    dates = []
    delta = end_date - start_date   # returns timedelta

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        dates.append(day)
    return dates


# function that creates time intervals from list of dates
def create_time_intervals(dates_list, Interval):
    time_df = pd.DataFrame({'Date': dates_list}).astype('datetime64[ns]')
    interval = timedelta(Interval)
    grouped_cr = time_df.groupby(pd.Grouper(key='Date', freq=interval))
    date_ranges = []
    for i in grouped_cr:
        date_ranges.append(((str(i[1].min()[0]).split(' ')[0]), (str(i[1].max()[0]).split(' ')[0])))
    return date_ranges


# function that creates median composite of sentinel data
def create_col(date_ranges, s2, roi):
    
    imgs = []
    
    # loop through 15-day steps
    for RANGE in date_ranges:
        
        # extract sentinel imagery between start and end of step dates
        s2_step = s2.filterDate(RANGE[0], RANGE[1]).filterBounds(roi)
        med_im = s2_step.reduce(ee.Reducer.median())
        med_im = med_im.set('system:time_start', ee.Date(RANGE[0]).millis()).set('empty', s2_step.size().eq(0))
        imgs.append(ee.Image(med_im))
        
    return ee.ImageCollection.fromImages(imgs)


# function that fills empty NDVI pixels with median NDVI
def fill_empty(im):
    return ee.Image(threshMin).rename('ndvi_median').copyProperties(im, ['system:time_start'])


# function that manages empty composites
def wrap_empty_window(col):
    
    def empty_window(im):
        
        # Window central day 
        date_window = ee.Date(im.get('system:time_start'))
        # Window first day 
        date_startW = date_window.advance(-INCREMENT*2, 'days')
        # Window last day 
        date_endW = date_window.advance(INCREMENT*2, 'days')
        # Compute mean with images before and after the central window 
        meanIm1 = col.filterDate(date_startW, date_window.advance(1, 'days')).reduce(ee.Reducer.mean())
        meanIm2 = col.filterDate(date_window.advance(-1, 'days'), date_endW).reduce(ee.Reducer.mean())
        meanIm = (meanIm1.add(meanIm2)).divide(2)
        
        return im.unmask(meanIm).copyProperties(im,['system:time_start'])
    
    return ee.ImageCollection(col.map(empty_window))


# function that samples rasters using specified points
def sample_raster(image, fcollection, scale=10, projection='EPSG:4326', geometries=False):
    fc = image.sampleRegions(collection = fcollection,
                             scale = scale,
                             projection = projection,
                             geometries = geometries)
    return fc


# function that converts feature collection to pandas dataframe
def fc_to_df(fc, idx_col):
    # Convert a FeatureCollection into a pandas DataFrame
    # Features is a list of dict with the output
    features = fc.getInfo()['features']

    dictarr = []

    for f in features:
        attr = f['properties']
        dictarr.append(attr)

    df = pd.DataFrame(dictarr)
    df.set_index(idx_col, inplace=True)
    return df

##################################################################################
##################################################################################

# Sentinel 2 cloud masking functions

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


##################################################################################
##################################################################################


for poly, idx in zip(polys, range(len(polys))):
    
    ROI = poly
    
    s2_sr_cld_col = get_s2_sr_cld_col(poi_fc, str(start_date), str(end_date))
    s2_sr = (s2_sr_cld_col.map(add_cld_shdw_mask).map(apply_cld_shdw_mask))
     
    S2 = s2_sr.select(['B12', 'B8', 'B4', 'B3', 'B2', 'SCL'])
    S2 = S2.map(func_nfe) # rescale and create NDVI

    date_ranges = create_time_intervals(create_list_of_dates(start_date, end_date), INCREMENT)
    imagecol = create_col(date_ranges, S2, ROI)
    filled = imagecol.filterMetadata('empty', 'equals', 1).map(fill_empty)
    imagecol = imagecol.filterMetadata('empty', 'equals', 0).merge(filled)
    imagecol = wrap_empty_window(imagecol)
    imagecol = imagecol.sort('system:time_start')

    ##################################################################################
    ##################################################################################

    # interpolate
    interp_imagecol = cubicInterpolation(imagecol, 1) # e.g. now I have ~120 images after having only 11

    init = ee.Image(ee.Date(str(year1-1) + '-12-31').millis())

    def func3(im): 
        return (im.rename('ndvi_interp')
                .addBands(im.metadata('system:time_start','date1'))
                .set('system:time_start',im.get('system:time_start')))
    interp = interp_imagecol.map(func3)

    minND = ee.Image(threshMin)
    maxND = imagecol.max()
    amplitude = maxND.subtract(minND)
    thresh = amplitude.multiply(th).add(minND).rename('ndvi_interp')

    def func4(im):
        out = im.select('ndvi_interp').gt(thresh)
        return im.updateMask(out)
    col_aboveThresh = interp.map(func4)

    SOS = col_aboveThresh.reduce(ee.Reducer.firstNonNull()).select('date1_first').rename('SOS')
    SOS_doy = SOS.subtract(init).divide(86400000)

    EOS = col_aboveThresh.reduce(ee.Reducer.lastNonNull()).select('date1_last').rename('EOS')
    EOS_doy = EOS.subtract(init).divide(86400000)

    LOS = EOS_doy.subtract(SOS_doy)

    ##################################################################################
    ##################################################################################

    points = all_points.filterBounds(ROI)

    # if there's actually any points to sample for an ROI:
    if points.size().getInfo():
        
        los = sample_raster(LOS, points)
        eos = sample_raster(EOS_doy, points)
        sos = sample_raster(SOS_doy, points)

        try:
            los_df = fc_to_df(los, 'uid')
            eos_df = fc_to_df(eos, 'uid')
            sos_df = fc_to_df(sos, 'uid')

        except Exception as e:
            print(e)
            continue

        df = pd.concat([sos_df, eos_df, los_df], axis=1)
        df = df.loc[:,~df.columns.duplicated()]
        df.drop(columns=['plot_size', 'year'], inplace=True)

        print(f'Completed {idx} of {len(admins)} hucs.')

        output_path = '/mnt/poseidon/remotesensing/arctic/data/training/huc190604_phenology_dates_01.csv'
        b = not os.path.exists(output_path)
        df.to_csv(output_path, mode='a', header=b)
        
    else:
        
        print(f'There are no points in {idx}')