# Python Jupyter Notebooks
These notebooks were used to clean and standardize source tables (sources: ava, akveg, abr, neon, nga). These notebooks are published with the Synthesis of field-based fractional vegetation cover observations across Arctic Alaska (in preparation). The goal of this work was to gather and harmonize plot data from disparate sources, with the end goal of using these plot data to train models that predict the plant fractional cover (PFT) for 8 PFTs: deciduous shrubs, evergreen shrubs, forbs, graminoids, litter, and non-vascular plants (broad category) including lichen and bryophytes (sub-categories). Thus, the data we used is limited to the scope of our study: arctic alaskan tundra within nine years of 2019 Sentinel-2 imagery.

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
    - plot_name [sring]: plot identification code usually created by the original data authors
    - deciduous_shrub_cover [float]: fractional cover of deciduous shrubs
	- deciduous_tree_cover [float]: fractional cover of deciduous trees
	- evergreen_shrub_cover [float]: fractional cover of evergreen shrubs
	- evergreen_tree_cover [float]: fractional cover of evergreen trees
	- forb_cover [float]: fractional cover of forbs
	- graminoid_cover [float]: fractional cover of graminoids
	- nonvascular_sum_cover [float]: fractional cover of non-vascular
	plants (bryophyteCover + lichenCover)
	- bryophyte_cover [float]: fractional cover of bryophytes
	- lichen_cover [float]: fractional cover of lichen
	- litter_cover [float]: fractional cover of litter
	- water_cover [float]: fractional cover of water
	- bareground_cover [float]: fractional cover of bare ground
	- other_cover [float]: fractional cover of miscellaneous "vegetation" like dead standing plants, algae, fungus, and cyanobacteria
	- survey_year [int]: year that survey was performed
	- survey_month [int]: month that survey was performed
	- survey_day [int]: day that survey was performed
	- plot_radius [float]: radius of a plot in meters
	- latitude_y [float]: latitude coordinate
	- longitude_x [float]: longitude coordinate
	- georef_source [string]: type of device used to collect coordinates
	- georef_accuracy [float]: accuracy of coordinates in meters
    - coord_EPSG [string]: coordinate system
	- data_subsource [string]: project in charge of data collection
	- data_source [string]: database code indicating where data was
	accessed from
	- survey_method [string]: the tactic employed for collecting field
	data
	- fcover_scale [string]: fractional cover unit used during field data
	collection
	- survey_purpose [string]: short description for the end goal of the
	project that funded data collection
	- geometry [geometry]: point coordinates
	- admin_unit [string]: state or province in which field data was
	collected
	- admin_country [string]: country in which field data was collected
	- fireYears [bool]: whether or not a fire occured where field data
	was collected
	- bioclim_subzone [int]: bioclimate subzone integer
	- duplicated_coords [bool]: whether or not the plot's coordinates
	appear more than once in the database
	- duplicated_date [bool]: whether or not the plot's collection date
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

9. **Export join of species names and their associated accepted names**

10. **Manually ID one accepted species name for each species name used in plot data tables**

11. **Aggregate species-level fcover to PFT-level fcover by grouping species into their PFTs and summing**
---
---
