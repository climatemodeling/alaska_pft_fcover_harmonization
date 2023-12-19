import ee
import logging
import multiprocessing
from retry import retry
import pandas as pd
import numpy as np
import geemap
import os
# https://gist.github.com/gorelick-google/cbd91d132964f39c4603b236919bceac

ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

# All the results will be stored here so we can output a single file at the end.
def getRequests():
    """Generates a list of work items to be downloaded.

    Extract the ADM2_CODEs from the GAUL level 2 dataset as work units.
    """
    # load observation points for later
    d = '/mnt/poseidon/remotesensing/arctic/data/vectors/AK-AVA_Turboveg/ak_tvexport_releves_header_data_for_vegbank_20181106_ALB.xlsx'
    obs_data = pd.read_excel(d, skiprows=[1])
    obs_data = obs_data.replace(-9, np.nan)
    
    obs_geom = obs_data[['Latitude (decimal degrees)', 'Longitude (decimal degrees)', 'Releve number']]
    obs_geom.set_index('Releve number', inplace=True)
    obs_points = geemap.df_to_ee(obs_geom, 
                                 latitude='Latitude (decimal degrees)', 
                                 longitude='Longitude (decimal degrees)')
    
    return obs_points.aggregate_array('system:index').getInfo()

@retry(tries=10, delay=1, backoff=2)
def getResult(index, regionID):
    """Handle the HTTP requests to download one result."""
    region = ee.Feature(ee.FeatureCollection("USGS/WBD/2017/HUC06")
            .filter(ee.Filter.eq('huc6', regionID))
            .first())

    def maxLST(image):
        # Mappable function to aggregate the max LST for one image.
        # It builds an output tuple of (max_LST, date, regionID)
        # This function uses -999 to indicate no data.
        date = image.date().format('YYYY-MM-dd')
        maxValue = (image.reduceRegion(ee.Reducer.median(), region.geometry())
                    # set a default in case there's no data in the region
                    .combine({'ndvi': -999}, False)
                    .getNumber('ndvi')
                    # format to 2 decimal places.
                    .format('%.2f'))

        return image.set('output', [maxValue, date, regionID])

    # calculate NDVI function
    def get_ndvi(im):
        ndvi = (im.normalizedDifference(['B8','B4'])
                .rename('ndvi')
                .set('system:time_start', im.get('system:time_start')))
        return ndvi

    # Get the max LST for this region, in each image.
    timeSeries = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                  .select(['B8', 'B4'])
                  .map(get_ndvi))
                  
    timeSeries = timeSeries.select('ndvi').map(maxLST)            
    result = timeSeries.aggregate_array('output').getInfo()

    # Write the results to a file.
    filename = '/mnt/poseidon/remotesensing/arctic/data/vectors/test/results_%d.csv' % regionID
    with open(filename, 'w') as out_file:
        for items in result:
            line = ','.join([str(item) for item in items])
            print(line, file=out_file)
    print("Done: ", index)


if __name__ == '__main__':
    logging.basicConfig()
    items = getRequests()
    print("after items")

    pool = multiprocessing.Pool(25)
    pool.starmap(getResult, enumerate(items))
    
    print("after pool")
    
    pool.close()
    pool.join()
    
    print("end of script")