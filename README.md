# Python Jupyter Notebooks
These notebooks were used to clean and standardize source tables (sources: ava, akveg, abr, neon, sp). These notebooks are published with the Fractional Cover Field Data Synthesis for Applications in Arctic Remote Sensing (in preparation). The goal of this work was to gather and harmonize plot data from disparate sources, with the end goal of using these plot data to train models that predict the plant fractional cover (PFT) for 8 PFTs: deciduous shrubs, evergreen shrubs, forbs, graminoids, litter, and non-vascular plants (broad category) including lichen and bryophytes (sub-categories). Thus, the data we used is limited to the scope of our study: arctic alaskan tundra within nine years of 2019 Sentinel-2 imagery.

---
## General procedure:

1. **Visually inspect and clean tables before standardization**
    - Rows: species names
    - Columns: plot IDs
    - Cell values: fcover

<br>

2. **Select plot data that were collected after 2010 and that are in the alaskan tundra**

3. **Read plot data into a pandas dataframe for tabular manipulation**

4. **Extract existing auxiliary data and add columns we want to include**
    - UID [int]: unique ID for every plot entry
    - PlotID [sring]: non-unique plot identification code usually created by the original data authors
    - Name [string]: code indicating the name of the database the plot data was stored on
    - Citation [string]: citation for a plot's study (when published as a paper)
    - AdminArea [string]: administrative area; locale indicating country, state/territories/provinces
    - Date [datetime]: collection date in a standard UTC format
    - Purpose [string]: data collection purpose
    - BioClimZone [string]: 2019 CAVM bioclimate zone
    - CoverScale [string]: cover scale like Braun-Blanquet that was used to indicate a range of fcover
    - SampleMethod [string]: how surveyors collected fcover data
    - GPSType [string]: the type of positioning system used to record plot centroids
    - GPSAccuracy [float]: accuracy of the positioning system used (in meters)
    - PlotSize [float]: the size of the plots (in radius)
    - FireHistory [bool]: whether or not a fire occurred at the plot site
    - Revisited [bool]: whether or not a plot site was revisted
    - Crs [string]: EPSG code indicating the geographic coordinate reference system of lat/lon
    - latitude [float]: Y coordinate
    - longitude [float]: X coordinate
    
<br>

5. **Correct fcover values that, when converted to float, raise a data-type error**
    - For example, "Trace" was sometimes used instead of 0.01
    - Random letters or special characters with no meaning
    
<br>

6. **Standardize fcover to a fraction (percent)**
    - Older plots used Braun-Blanquet or Westoff codes that indicate a range of fcover; the mid-point percentage was used for these
    
<br>

7. **Join the species names from the fcover data to the species name in the AKVEG Species Checklist to identify potential habits and accepted species names**

8. **Join species names from the fcover data to the species name in the Leaf Habit Supplementary Table from Macander et al. (2020) to assign shrubby plants a leaf retention type (deciduous, evergreen)**

9. **Export join species names and their associated accepted names**

10. **Manually ID one accepted species name for each species name used in plot data tables**

11. **Aggregate species-level fcover to PFT-level fcover by grouping species into their habits and summing**
---
---