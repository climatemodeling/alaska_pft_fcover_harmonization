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
    - plotName [sring]: plot identification code usually created by the original data authors
    - deciduousShrubCover [float]: fractional cover of deciduous shrubs
	- deciduousTreeCover [float]: fractional cover of deciduous trees
	- evergreenShrubCover [float]: fractional cover of evergreen shrubs
	- evergreenTreeCover [float]: fractional cover of evergreen trees
	- forbCover [float]: fractional cover of forbs
	- graminoidCover [float]: fractional cover of graminoids
	- nonvascularSumCover [float]: fractional cover of non-vascular
	plants (bryophyteCover + lichenCover)
	- bryophyteCover [float]: fractional cover of bryophytes
	- lichenCover [float]: fractional cover of lichen
	- litterCover [float]: fractional cover of litter
	- waterCover [float]: fractional cover of water
	- baregroundCover [float]: fractional cover of bare ground
	- surveyYear [int]: year that survey was performed
	- surveyMonth [int]: month that survey was performed
	- surveyDay [int]: day that survey was performed
	- plotRadius [float]: radius of a plot in meters
	- latitudeY [float]: latitude coordinate
	- longitudeX [float]: longitude coordinate
	- georefSource [string]: type of device used to collect coordinates
	- georefAccuracy [float]: accuracy of coordinates in meters
    - coordEPSG [string]: coordinate system
	- dataSubsource [string]: project in charge of data collection
	- dataSource [string]: database code indicating where data was
	accessed from
	- surveyMethod [string]: the tactic employed for collecting field
	data
	- fcoverScale [string]: fractional cover unit used during field data
	collection
	- surveyPurpose [string]: short description for the end goal of the
	project that funded data collection
	- geometry [geometry]: point coordinates
	- adminUnit [string]: state or province in which field data was
	collected
	- adminCountry [string]: country in which field data was collected
	- fireYears [bool]: whether or not a fire occured where field data
	was collected
	- bioclimSubzone [int]: bioclimate subzone integer
	- duplicatedCoords [bool]: whether or not the plot's coordinates
	appear more than once in the database
	- duplicatedDate [bool]: whether or not the plot's collection date
	appears more than once in the database 

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
