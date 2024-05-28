import pandas as pd
import numpy as np


##########################################################################################
# Python3 Example
##########################################################################################

"""
# set vars
out_path = '/mnt/poseidon/remotesensing/arctic/data/vectors/NEON_2021'

# get data
p = '/mnt/poseidon/remotesensing/arctic/data/vectors/NEON_2021/'
f = 'NEON.D18.TOOLBARR.DP1.10058.001.div_1m2Data.2021.csv'
neon = pd.read_csv(p + f, index_col='Unnamed: 0')
print(neon.columns)

# get unique species list
species = get_unique_species(DFRAME=neon, 
                             SCOL='scientificName', 
                             DNAME='NEON', 
                             SAVE=True, 
                             OUTP=out_path)

# get species ancillary info
ancillary_cols = ['namedLocation', 'domainID', 'siteID', 
                  'geodeticDatum', 'coordinateUncertainty',
                  'elevation', 'elevationUncertainty', 'plotType',
                  'nlcdClass', 'plotID', 'subplotID', 'endDate',
                  'boutNumber', 'eventID', 'samplingProtocolVersion',
                  'name', 'subplot_lat', 'subplot_lon']
                  
ancillary = get_species_ancillary(DFRAME=neon,
                                  ANC_COLS=ancillary_cols,
                                  DNAME='NEON',
                                  SAVE=True,
                                  OUTP=out_path)
                                  
# load species checklist
p = '/mnt/poseidon/remotesensing/arctic/data/vectors/AKVEG_ACCS/'
f = 'AKVEG_species_checklist.csv'
checklist = read_dataframe(p + f)

# get first 2 words (genus-species) from checklist accepted name and data species name
checklist['Mapping Name'] = checklist['Accepted Name'].apply(get_substrings)
species['Mapping Name'] = species['scientificName'].apply(get_substrings)

# attach potential habits to each species
habits = fill_habits(unique_species=species, 
                     checklist=checklist, 
                     u_name='scientificName', 
                     c_unofficial_name='Name', 
                     c_official_name='Accepted Name', 
                     mapping_name='Mapping Name',
                     habit='Habit')

# add evergreen/deciduous information to each species
p = '/mnt/poseidon/remotesensing/arctic/data/vectors/AK-AVA_post2000/'
f = 'evergreendecid_macander2022.csv'
evergrndecid = pd.read_csv(p + f, header=None)
evergrndecid.columns = ['evergreendecid', 'species']
final = add_leaf_habit(habits, evergrndecid)

# clean up
final['Potential Habit'] = final['Habit']
final[['Potential Height', 'Height', 'Habit']] = np.NaN
final = final[['Name', 'Mapping Name', 'Potential Habit', 
               'Habit', 'Leaf Habit', 'Potential Height', 
               'Height']]

# export
p = '/mnt/poseidon/remotesensing/arctic/data/vectors/NEON_2021/'
f = 'NEON_species_habit_00.csv'
final.to_csv(p + f)
"""


##########################################################################################
# Read Data
##########################################################################################

# get a column of unique species in a dataset
def get_unique_species(DFRAME, SCOL, DNAME, SAVE=False, OUTP=False):

    # Get unique species names
    unique_species = DFRAME.groupby([SCOL]).first()
    unique_species = unique_species.reset_index()
    unique_species_df = pd.DataFrame(unique_species[SCOL])
    
    # Export
    if SAVE:
        if not OUTP:
            print('OUTP requires path to output folder.')
        else:
            try:
                path = f'{OUTP}/{DNAME}_unique_species.csv'
                unique_species_df.to_csv(path)
                print(f'Saved unique species list to {path}.')
            except Exception as e:
                print(e)
                
    # Return dataframe
    return unique_species_df

# get ancillary data from species data
def get_species_ancillary(DFRAME, ANC_COLS, DNAME, SAVE=False, OUTP=False):
    
    # Extract species-level ancillary data
    ancillary = DFRAME[ANC_COLS]
    
    # Export
    if SAVE:
        if not OUTP:
            print('OUTP requires path to output folder.')
        else:
            try:
                path = f'{OUTP}/{DNAME}_species_ancillary.csv'
                ancillary.to_csv(path)
                print(f'Saved species-level ancillary data to {path}.')
            except Exception as e:
                print(e)
                
    # Return dataframe
    return ancillary

# assign habit to species
def create_checklist_habits(checklist, mapping_name, habit):
    
    # For every genus-species, create a list of potential habits
    checklist_merge = (checklist
                       .groupby(mapping_name)[habit]
                       .apply(set)
                       .reset_index())
    
    # Make lists strings, remove brackets, remove whitespace
    checklist_merge[habit] = (checklist_merge[habit]
                              .astype(str)
                              .str.strip('{}')
                              .str.strip("''")
                              .str.strip())
    
    # clean up the strings created above
    checklist_merge[habit] = checklist_merge[habit].apply(cleanlist)
    
    # Return dataframe
    return checklist_merge

# add leaf habit to species using macander 22 supplementary crosswalk
def add_leaf_retention(species_pft, evergrndecid, ret_col_name):
    
    # add evergreen/deciduous information
    species_pft_l = species_pft.to_numpy()
    evergrndecid_l = evergrndecid.to_numpy()
    
    newlist = []
    for row in species_pft_l:

        # get first 2 words in NEON species name
        speciesname = row[1].split()[:2]
        newrow = []

        for row2 in evergrndecid_l:
            # get first 2 words in evergreen/decid species name
            speciesname2 = row2[1].split()[:2]
            ed = None

            # if full NEON name == E/D name:
            if speciesname == speciesname2:
                #get evergreen/decid
                ed = row2[0].split()
                ed = ed[0]

            # if NEON genus == E/D genus:
            elif str(speciesname[0]) == str(speciesname2[0]):
                #print(speciesname[0], 'and', speciesname2[0])
                ed = row2[0].split()
                ed = ed[0]

            # if they don't match, dont append
            else:
                continue
            newrow.append(ed)

        newlist.append(newrow)
    
    # get unique leaf habit values
    final_l = []
    for l in newlist:
        newl = list(set(l))
        string = ','.join(newl)
        final_l.append(string)
        
    species_pft[ret_col_name] = final_l
    return species_pft

# connect habit information to species names using AKVEG species checklist
# connect habit information to species names using AKVEG species checklist
def join_to_checklist(unique_species, checklist, u_name, c_unofficial_name, 
                      c_official_name, mapping_name, habit):
    
    ######################################################################################
    # compare genus-species to official name genus-species
    ######################################################################################
    # extract first two words (genus & species) of official name
    unique_species[mapping_name] = unique_species[u_name].apply(get_substrings)
    checklist[mapping_name] = checklist[c_official_name].apply(get_substrings)

    # match unofficial name to blank habit species
    checklist1 = create_checklist_habits(checklist=checklist, 
                                         mapping_name=mapping_name, 
                                         habit=habit)

    # get habit for species based on unofficial name
    species_pft = (unique_species
                   .reset_index()
                   .merge(checklist1, how='left', on=mapping_name)
                   .set_index('index'))

    # check if species habit is blank
    species_pft1 = species_pft.replace('', np.nan)
    missinghabit1 = species_pft1.loc[species_pft1.isna().any(axis=1)]
    print(f'{len(missinghabit1)} species are missing habits.')
    
    ######################################################################################
    # compare genus-species to UNofficial name genus-species
    ######################################################################################
    # extract first two words (genus & species) of official name
    checklist2 = checklist.copy()
    checklist2[mapping_name] = checklist2[c_unofficial_name].apply(get_substrings)

    # match unofficial name to blank habit species
    checklist2 = create_checklist_habits(checklist=checklist2, 
                                         mapping_name=mapping_name, 
                                         habit=habit)

    # get habit for species based on unofficial name
    species_pft2 = (missinghabit1
                    .reset_index()
                    .merge(checklist2, how='left', on=mapping_name, suffixes=['1','2'])
                    .set_index('index'))

    # show species that are still missing habits
    newhabit2 = f'{habit}2'
    missinghabit2 = species_pft2[species_pft2[newhabit2].isnull()]
    print(f'{len(missinghabit2)} species still missing habits.')
    
    ######################################################################################
    # compare genus to official name genus
    ######################################################################################
    # extract genus of official name and species name
    checklist3 = checklist.copy()
    checklist3[mapping_name] = checklist3[mapping_name].apply(get_first_substring)
    missinghabit3 = missinghabit2.copy()
    missinghabit3[mapping_name] = missinghabit2[mapping_name].apply(get_first_substring)

    # match unofficial name to blank habit species
    checklist3 = create_checklist_habits(checklist=checklist3,
                                         mapping_name=mapping_name,
                                         habit=habit)

    # get habit for genus
    species_pft3 = (missinghabit3
                    .reset_index()
                    .merge(checklist3, how='left', on=mapping_name, suffixes=['2','3'])
                    .set_index('index'))

    # show species that are still missing habits
    newhabit3 = habit
    missinghabit3 = species_pft3[species_pft3[newhabit3].isnull()]
    print(f'{len(missinghabit3)} species still missing habits.')
    
    ######################################################################################
    # compare genus to unofficial name genus
    ######################################################################################
    # extract genus of unofficial name and species name
    checklist4 = checklist.copy()
    checklist4[mapping_name] = checklist4[c_unofficial_name].apply(get_first_substring)
    missinghabit4 = missinghabit3.copy()
    missinghabit4[mapping_name] = missinghabit3[mapping_name].apply(get_first_substring)

    # match unofficial name to blank habit species
    checklist4 = create_checklist_habits(checklist=checklist4,
                                         mapping_name=mapping_name,
                                         habit=habit)

    # get habit for genus
    species_pft4 = (missinghabit3
                    .reset_index()
                    .merge(checklist4, how='left', on=mapping_name, suffixes=['3','4'])
                    .set_index('index'))

    # show species that are still missing habits
    newhabit4 = f'{habit}4'
    missinghabit4 = species_pft4[species_pft4[newhabit4].isnull()]
    print(f'{len(missinghabit4)} species still missing habits.')
    
    ######################################################################################
    # fill missing habit names
    ######################################################################################
    # set up columns for filling
    fill1 = species_pft2[[newhabit2]]
    fill2 = species_pft3[[newhabit3]]
    fill2 = fill2.copy()
    fill2.rename(columns={newhabit3: f'{habit}3'}, inplace=True)
    fill3 = species_pft4[[newhabit4]]
    finalhabits = pd.concat([species_pft, fill1, fill2, fill3], axis=1)
    
    # fill
    finalhabits[habit] = finalhabits[habit].fillna(finalhabits[newhabit2])
    finalhabits[habit] = finalhabits[habit].fillna(finalhabits[f'{habit}3'])
    finalhabits[habit] = finalhabits[habit].fillna(finalhabits[newhabit4])
    finalhabits = finalhabits[[u_name, mapping_name, habit]]
    finalhabits.columns = [u_name, mapping_name, habit]
    
    # return dataframe
    return finalhabits


# aggregate to final PFT schema
def agg_to_pft_schema(df):

    # extract evergreen/decid shrub/tree col names
    evergreen = sorted([col for col in df.columns if all(k in col for k in ['evergreen dwarf'])])
    deciduous = sorted([col for col in df.columns if any(k in col for k in ['deciduous dwarf'])])
    evergtree = sorted([col for col in df.columns if all(k in col for k in ['evergreen tree'])])
    decidtree = sorted([col for col in df.columns if all(k in col for k in ['deciduous tree'])])
    # nonvasc = sorted([col for col in df.columns if any(k in col for k in ['lichen', 'bryophyte'])])

    # select cols and set as new dataframe
    e_shrub = df[evergreen]
    d_shrub = df[deciduous]
    e_tree = df[evergtree]
    d_tree = df[decidtree]
    # n_vasc = test[nonvasc]

    # sum sub-categories into new main category and select
    # shrubs
    e_shrub['evergreen shrub total cover (%)'] = e_shrub.sum(axis=1)
    e_shrub = e_shrub[['evergreen shrub total cover (%)']]
    d_shrub['deciduous shrub total cover (%)'] = d_shrub.sum(axis=1)
    d_shrub = d_shrub[['deciduous shrub total cover (%)']]
    # trees
    e_tree['evergreen tree total cover (%)'] = e_tree.sum(axis=1)
    e_tree = e_tree[['evergreen tree total cover (%)']]
    d_tree['deciduous tree total cover (%)'] = d_tree.sum(axis=1)
    d_tree = d_tree[['deciduous tree total cover (%)']]
    #non-vascular
    # n_vasc['non-vascular total cover (%)'] = n_vasc.sum(axis=1)
    # n_vasc = n_vasc[['non-vascular total cover (%)']]

    # drop old sub-columns and replace with new
    subcats = evergreen + deciduous + evergtree + decidtree # + n_vasc
    cats = df.drop(columns=subcats) # get everything but leafy plants
    aggregated = pd.concat([e_shrub, 
                            e_tree, 
                            d_shrub, 
                            d_tree, 
                            # n_vasc,
                            cats], 
                           axis=1)
    
    cover = sorted([col for col in aggregated if 'cover' in col])
    other = sorted([col for col in aggregated if 'cover' not in col])
    c = aggregated[cover]
    o = aggregated[other]
    newdf = pd.concat([c,o], axis=1)
        
    return newdf

    
##########################################################################################
# Pandas row-wise functions to use with .apply()
##########################################################################################

# function to get genus and species name
def get_substrings(row):
    
    # extract genus + species name
    words = row.split()[:2]
    if 'species' in words: # if just genus
        string = words[0]
    elif 'Unknown' in words: # if unknown species
        string = words[1]
    else:
        string = ' '.join(words)
        
    # remove potential brackets in string
    string = string.replace('[','').replace(']','')
    return string

# function get genus name only
def get_first_substring(row):
    
    # extract genus name
    words = row.split()[:1]
    string = ' '.join(words)
    return string

# get unique values in a list
def uniquelist(row):
    rowlist = list(row)
    unique = set(rowlist)
    return list(unique)

# clean list appearance
def cleanlist(row):
    new = row.strip().replace("'", '')
    newlist = new.split(',')
    newlist = list(set(newlist))
    return ','.join(newlist)

# flatten multilevel
def flatten_multilevel(grouped):
    
    """
    Flattens a multi-index pandas dataframe with plots (idx0) 
    and associated habit fcovers (idx1).
    Returns a single index dataframe.
    """
    
    grouped.columns = grouped.columns.get_level_values(0)
    grouped = grouped.reset_index()
    return grouped

# transpose
# this function is ABR-specific
def transpose_df(grouped, habit_col):
    
    """
    Transposes a flattened multi-index dataframe so that plotIDs
    are rowwise and columns are standardized habits. Cells contain
    aggregated fcover values.
    Returns a reset single index dataframe.
    """
    
    groups = grouped.set_index(['plot_id', habit_col]).stack().unstack([1,2])
    groups.columns = groups.columns.get_level_values(0)
    groups = groups.reset_index()
    return groups

# all required column names
# if a column doesn't exist, add it and fill with nan fcover
def add_standard_cols(groups):
    
    # required columns
    necessary_cols = ['deciduous dwarf shrub cover (%)',
                      'deciduous dwarf to low shrub cover (%)',
                      'deciduous dwarf to tall shrub cover (%)',
                      'deciduous dwarf to tree cover (%)',
                      'deciduous tree cover (%)',
                      'evergreen dwarf shrub cover (%)',
                      'evergreen dwarf to low shrub cover (%)',
                      'evergreen dwarf to tall shrub cover (%)',
                      'evergreen dwarf to tree cover (%)',
                      'evergreen tree cover (%)',
                      'bryophyte cover (%)',
                      'forb cover (%)',
                      'graminoid cover (%)',
                      'lichen cover (%)']
    
    # add missing columns and fill with nan
    cols = groups.columns.tolist()
    addcols = []
    for nc in necessary_cols:
        if nc not in cols:
            addcols.append(nc)
    groups[addcols] = np.nan
    return groups




