# %% [markdown]
# # Validate 
# Author: Mark Chinnock  
# Date: 19/07/2024
# 
# This script can read in excel file(s) from sharepoint that have previously been extracted from 3dx, processed to add the additional functions/metrics and passed to smartsheets.  Currently, this is what it is doing as I am expecting to process the historic files to create summary metrics.
# 
# Going forward this could read in the 3dx extract directly to validate the latest attributes.
# 
# It can process as many files as you want, for as many product structures as you want, and build a history of data quality.  Currently writing out to excel file named the same as the input file and suffixed with '_validated'.
# 
# Required Inputs:
# * 3dx extract xlsx file - as long as the standard columns used for calculating the metrics are present this will process it.
# 
# Outputs Produced:
# * xlsx spreadsheet with full input BOM written to a sheet with columns for each metric appended to far right columns for reporting in power bi/excel
# * Additional sheets written into xlsx for each validation rule, showing the subset of records that failed each rule (might be useful for quick dashboard displays)

# %% [markdown]
# # 1. Validation Rules

# %% [markdown]
# ## 1.1 Makes without Buys
# For each Make part there should be an assembly that needs at least one Buy part below it to make sense - if you're going to bake a cake, you need at least 1 ingredient!  If you're buying a cake, then you don't need anything else!
# 
# If a MAKE (source code 'AIH','MIH','MOB') is not followed by a child BUY this is a problem

# %%
def check_make_no_buy(df):
    # if MAKE and there is no BUY below next row's part level less than or equal to current part level we have a MAKE without a BUY
    # df['PROVIDE'] = np.where(df['Source Code'].isin(['AIH','MIH','MOB']),'Make','Buy')
    make_no_buy = list(df[(df['Source Code'].isin(['AIH','MIH','MOB'])) & (df['Level'].shift(-1) <= df['Level'])].index)
    make_no_buy = sorted(make_no_buy)
    df['make_no_buy'] = np.where((df['Source Code'].isin(['AIH','MIH','MOB'])) & (df['Level'].shift(-1) <= df['Level']), True, False)

    return df, make_no_buy

# %% [markdown]
# ## 1.2 Parent Source Code
# 
# We will need to track the source code of the parent part and use it for validate checks coming later.
# 
# This interates through the dataframe and appends to each row the parent part source code (the level numerically above this row's level)

# %%
def parent_source_code(df):
    prev_level = 0

    level_source = {}

    for i, x in df.iterrows():
        # take the current level source and store it
        level_source[x['Level']] = x['Source Code']
        if ((x['Level'] >= 4)):
            df.loc[i, 'Parent Source Code'] = level_source[x['Level'] - 1]
    
    return df

# %% [markdown]
# ## 1.3 Source Code within parent checks
# 
# sc_check_list is a list of invalid scenarios we are checking for with syntax: SOURCE CODE_PARENT SOURCE CODE
# 
# A dataframe of invalid rows is written to dict_checks[sc_check] and a column is added to the end of the main dataframe with the sc_check as a column name holding true or false

# %%
def source_code_within_parent_checks(dict_checks, df):
    # check for combinations of source codes within a parent source that's not accepted
    sc_check_list = ['AIH_POA','BOP_FIP','FAS_FAS','FIP_FIP','FIP_FAS']
    
    for sc_check in sc_check_list:
        sc, parent_sc = sc_check.split('_')

        dict_checks[sc_check] = df[(df['Source Code'] == sc) & (df['Parent Source Code'] == parent_sc)]

        df[sc_check] = np.where((df['Source Code'] == sc) & (df['Parent Source Code'] == parent_sc), True, False)


    return dict_checks, df

# %% [markdown]
# ## 1.4 Level 4 Source Code Checks
# 
# level 4 (assembly level when first level = 0) should only have Source Code 'MIH' or 'AIH'

# %%
def check_level_4_source_code_checks(dict_checks, df):
    # level 4 can only be MIH or AIH
    dict_checks['Non_MIH_AIH_Level_4'] = df[(df['Level'] == 4) & (~df['Source Code'].isin(['MIH','AIH']))]
    df['Non_MIH_AIH_Level_4'] = np.where((df['Level'] == 4) & (~df['Source Code'].isin(['MIH','AIH'])), True, False)

    return dict_checks, df

# %% [markdown]
# ## 1.5 Fasteners with wrong parent source code
# 
# Fasteners should only be within parents of 'FIP','AIH,'MIH'
# 
# 

# %%
def FAS_wrong_parent_source_code(dict_checks, df):
    # FAS can only be within a FIP, AIH or MIH parent
    dict_checks['FAS_Wrong_Parent_Source_code'] = df[(df['Source Code'] == 'FAS') & (~df['Parent Source Code'].isin(['FIP','AIH','MIH']))]
    df['FAS_Wrong_Parent_Source_code'] = np.where((df['Source Code'] == 'FAS') & (~df['Parent Source Code'].isin(['FIP','AIH','MIH'])), True, False)

    return dict_checks, df


# %% [markdown]
# ## 1.6 Fastener checks
# 
# Look for scenarios where a description says washer, bolt or grommet but the source code says 'BOF'.  

# %%
def fastener_checks(dict_checks, df):
    # All BOF records that are fasteners should be {FAS}teners in the BOMS
    # Part Description contains washer, bolt, grommet
    # Source code = "BOF"
    fastener_check_list = ['^washer|^bolt|^grommet']        

    dict_checks['FAS_as_BOF'] = df[(df['Description'].str.lower().str.contains('{}'.format(fastener_check_list))) & (df['Source Code'] == 'BOF')]
    df['FAS_as_BOF'] = np.where((df['Description'].str.lower().str.contains('{}'.format(fastener_check_list))) & (df['Source Code'] == 'BOF'), True, False)

    return dict_checks, df

# %% [markdown]
# ## 1.7 Filter check columns
# 
# For writing out to excel on separate sheets, only need to keep the pertinent columns

# %%
def filter_check_columns(dict_checks):
    # reduce the selection of columns used for writing out later
    check_columns = [
    'orig_sort',
    'Last Modified By',
    'Owner',
    'Function Group',
    'System',
    'Sub System',
    'Level',
    'Title',
    'Revision',
    'Description',
    'Parent Part',
    'Source Code',
    'Quantity',
    'Parent Source Code'
    ]

    for key in dict_checks.keys():
        # print (key)
        dict_checks[key] = dict_checks[key][check_columns]

    return dict_checks

# %% [markdown]
# ## 1.8 Validate GMT Standards 3DX Attributes Requirements
# 
# Read in the GMT Standards document from GMT sharepoint folder and confirm 3dx extract contains the same columns and valid values
# 
# 

# %%
def check_attributes(df, attr_filename):


    attr = pd.read_excel(attr_filename, sheet_name='Drop Down Attributes', na_values="", keep_default_na=False)

    # create a dictionary of all the valid values for each column - this drops the nan values for each column
    attr_d = {attr[column].name: [y for y in attr[column] if not pd.isna(y)] for column in attr}
    

    for key in attr_d:
        # check the column exists
        try:
            mask = df[key].isin(attr_d[key])
            df['Check ' + key] = np.where(mask, 'Valid','Invalid')
        except KeyError as e:
            df[key] = 'Not in Extract'

    return df


# %% [markdown]
# # 1.9 Validate BoM and Function Group Structure GMT document
# 
# This document is stored on GMT-EngineeringBoM sharepoint in GMT Standards folder:  
# https://forsevengroup.sharepoint.com/:x:/r/sites/GMT-EngineeringBoM/Shared%20Documents/GMT%20-%20Standards/BoM%20and%20Function%20Group%20Structure%20GMT.xlsx?d=wc3cbfc77631c40b69ba7d5026066a2e7&csf=1&web=1&e=B64OP2

# %%
def validate_BOM_Function_Group_structure():
    # this is incomplete
    struct_filename = 'BoM and Function Group Structure GMT.xlsx'

    struct = pd.read_excel(struct_filename, sheet_name='T48E')
    # drop first row of struct which should be project, model variant, function group area, systems, sub sytems, AMs
    struct = struct.loc[1:]

    # find the first nan row in Level 4s as this will show where the last row in the valid values ends
    last_row = struct['Level 4'].isna().idxmax()-1

    # drop the rest of the struct rows
    struct = struct.loc[:last_row]

    # and now create a dictionary of all the valid values for each column - this drops the nan values for each column where we read merged cells from excel
    struct_d = {struct[column].name: [y for y in struct[column] if not pd.isna(y)] for column in struct}



# %% [markdown]
# # 1.10 Validate Part No
# 
# at the moment the Part Number (Title) should be:
# 
# [project]-[functional area][5 digit part number] == 11 characters

# %%
def validate_part_no(df):
    # ? means the preceding bracketed group is optional (optional s,S and trailing X)
    pattern = r'([A-Z]\d{2}[e])-(\w[A-Za-z0-9]*)?-?([A-Z])(\d{5})(X)?'
    df[['extr_project','extr_invalid_code','extr_function','extr_pn','extr_maturity']] = df['Title'].str.extract(pattern, expand=True)

    return df

# %% [markdown]
# # 2. Script config setup

# %%
import pandas as pd
import numpy as np
import os
import re
import io
import xlwings as xw
import openpyxl
from pathlib import Path
import argparse
import platform
import sys
import configparser


# %% [markdown]
# function to determine whether we're running in Juypter notebook or as a command line script

# %%
def type_of_script():
    '''
        determine where this script is running
        return either jupyter, ipython, terminal
    '''
    try:
        ipy_str = str(type(get_ipython()))
        if 'zmqshell' in ipy_str:
            return 'jupyter'
        if 'terminal' in ipy_str:
            return 'ipython'
    except:
        return 'terminal'

# %%
def add_missing_metrics(df):
    for col in ['UOM','Provide','Source Code','CAD Mass','CAD Maturity','CAD Material','Electrical Connector']:
        df['Missing {}'.format(col)] = np.where(df[col].isnull(), 1, 0)

    return df

# %%
def CAD_Material_validation(df):
    df[1:][df['Title'].str.contains('TPP', na=False)].groupby(['Title','CAD Material']).size()


# %%
def add_bi_key(df):
    # for use in power_bi reporting
    # replace NaN with ''
    df['bi_combined_key'] = df['Product'].astype(str) + df['Function Group'].astype(str) + df['System'].astype(str) + df['Sub System'].astype(str)
    
    return df['bi_combined_key']

# %% [markdown]
# determine the folder structure based on whether we're running on a test windows pc, in azure server, a mac, or in the real world against sharepoint - helps Mark test on different devices! 

# %%
def set_folder_defaults():
    if 'macOS' in platform.platform():
        # set some defaults for testing on mac
        download_dir = Path('/Users/mark/Downloads')
        user_dir = download_dir
        sharepoint_dir = download_dir

    elif 'Server' in platform.platform():
        # we're on the azure server (probably)
        user_dir = Path('Z:/python/FilesIn')

        download_dir = Path(user_dir)
        user_dir = download_dir
        sharepoint_dir = Path('Z:/python/FilesOut')

    elif os.getlogin() == 'mark_':
        # my test windows machine
        download_dir = Path('C:/Users/mark_/Downloads')
        user_dir = download_dir
        sharepoint_dir = download_dir        

    else:
        # personal one drive
        user_dir = 'C:/Users/USERNAME'

        # replace USERNAME with current logged on user
        user_dir = user_dir.replace('USERNAME', os.getlogin())

        # read in config file
        config = configparser.ConfigParser()
        config.read('user_directory.ini')

        # read in gm_dir and gm_docs from config file
        gm_dir = Path(config[os.getlogin().lower()]['gm_dir'])
        gm_docs = Path(config[os.getlogin().lower()]['gmt'])
        # this may find more than one sharepoint directory
        # sharepoint_dir = user_dir + "/" + gm_dir + "/" + gm_docs
        sharepoint_dir = Path(user_dir / gm_dir / gm_docs)

        # download_dir = os.path.join(sharepoint_dir, 'Data Shuttle', 'downloads')
        download_dir = Path(sharepoint_dir / 'Data Shuttle' / 'downloads')

    return sharepoint_dir, download_dir, user_dir

# %% [markdown]
# based on the folder defaults look for the files we're interested in

# %%
def find_files(download_dir):
    # find any changed files changed in past 2hrs in the downloads directory
    dirpath = download_dir
    files = []
    for p, ds, fs in os.walk(dirpath):
        for fn in fs:
            if 'Updated_' in fn:
                # was using this to filter what filenames to find
                filepath = os.path.join(p, fn)
                files.append(filepath)

    return files

# %%
def lookup_variant(search):

    variant_d = {'T48E-01-Z00001':'VP_5_door',
                'T48E-01-Z00005':'XP_5_door',
                'T48E-02-Z00001':'VP_3_door',
                'T48E-02-Z00005':'XP_3_door'}
    try:
        variant = variant_d[search.upper()]
    except KeyError:    
        print ("No variant lookup found for {} Therefore didn't update variant name".format(search.upper()))
        # just return what we searched with
        variant = search
    return variant

# %% [markdown]
# # 3. Write to excel
# 
# Call xlwings with your pre-prepared dictionary and write out many sheets to one excel file, naming the sheets whatever you called your dictionary keys

# %%
def write_to_xl(output_file, dict_checks):

    outfile = output_file
    df_dict = dict_checks

    import xlwings as xw
    try:
        wb = xw.Book(output_file)
        print ("writing to existing {}".format(outfile))
    except FileNotFoundError:
        # create a new book
        print ("creating new {}".format(outfile))
        wb = xw.Book()
        wb.save(outfile)

    for key in df_dict.keys():
        try:
            ws = wb.sheets.add(key)
        except Exception as e:
            print (e)
        
        ws = wb.sheets[key]

        table_name = key

        ws.clear()

        df = df_dict[key].set_index(list(df_dict[key])[0])
        if len(df) > 0:
            if table_name in [table.df for table in ws.tables]:
                ws.tables[table_name].update(df)
            else:
                table_name = ws.tables.add(source=ws['A1'],
                                            name=table_name).update(df)

# %% [markdown]
# # write out to excel using sub system
# 
# This was used previously (GMD) might be useful again so haven't removed, but not currently calling.
# 
# Writes out the checks to sheets filtered against the sub system - maybe useful if we wanted to give the problem rows to a team to manage

# %%
def write_to_xl_sub_system(dict_checks):
    sub_sys = dict_checks[check]['Sub System'].unique()
    sub_sys.sort()

    for s_sys in sub_sys:

        df_temp = dict_checks[check][dict_checks[check]['Sub System'] == s_sys]

        if df_temp.shape[0] > 0:
            df_temp.to_excel(writer, sheet_name=s_sys, index=False)

            ws = writer.sheets[s_sys]
            wb = writer.book

            excel_formatting.adjust_col_width_from_col(ws)

# %% [markdown]
# # 4. Main Processing
# 
# This is where the processing begins, and where we call the functions defined above.  

# %%
def main(df):

    # copy df without the 1st row of metrics and 1st col of BOM COUNTs
    # safer to search for percent_missing row and get rid of it
    df = df[df.orig_sort != 'percent_missing']
    # BOM COUNT is not needed 
    try:
        df.drop(columns='BOM COUNT', inplace=True)
    except KeyError:
        # didn't find BOM COUNT - doesn't matter
        pass

    df.reset_index(drop=False, inplace=True)
    df.rename(columns={'index':'bom_order'}, inplace=True)

    # variant ie T48E-01-Z00001
    # variant should be the first title in this dataframe, or the level 0 title
    variant = lookup_variant(df['Title'][df['Level']==0].values[0])
    # product ie T48E should be first part of the Title we got in variant
    product = re.split('-', df['Title'][df['Level']==0].values[0])[0].upper()


    # add variant column for merging multiple BOMs together and reporting on 1 dashboard
    df['Variant'] = variant
    df['Product'] = product
    
    df['bi_combined_key'] = add_bi_key(df)    

    df = add_missing_metrics(df)
    # add part validation
    df = validate_part_no(df)

    # add parent source code to each row for validation checks to come
    df = parent_source_code(df)

    # initialise a dictionary to store all the check output as dataframes
    dict_checks = {}

    # complete the source code with parent source code checks
    dict_checks, df = source_code_within_parent_checks(dict_checks, df)

    # check all level 4 have the correct source code
    dict_checks, df = check_level_4_source_code_checks(dict_checks, df)

    # check for FAS with the wrong source code
    dict_checks, df = FAS_wrong_parent_source_code(dict_checks, df)

    # complete the fasteners source code checks
    dict_checks, df = fastener_checks(dict_checks, df)

    # look for make assemblys with no parts to buy
    df, make_no_buy = check_make_no_buy(df)
    dict_checks['make_no_buy'] = df.loc[make_no_buy]

    # set folder defaults again in case called directly from another script
    sharepoint_dir, download_dir, user_dir = set_folder_defaults()
    
    # validate the 3dx attributes that have dropdowns
    attr_file = Path(sharepoint_dir / 'GMT - Standards' / '3DX Attributes Requirements for Release and Clarification.xlsx')
    df = check_attributes(df, attr_file)

    # write out just the cols we need to report against
    dict_checks = filter_check_columns(dict_checks)

    # add the full df to the sheet
    dict_checks['BOM'] = df

    # build outfile name and write to excel for power bi
    # outfile_name = product + '_' + variant
    # output_file = Path(sharepoint_dir) / 'power_bi' / Path(outfile_name + '_power_bi_metrics').with_suffix('.xlsx')
    # write_to_xl(output_file, dict_checks)

    return dict_checks

# %%
# this gets called if running from this script.  
if __name__ == '__main__':

    # for reading in multiple files

    # files = find_files()
    dict_df = {}

    filename = 'T48E/Updated_T48e-01-Z00001_2024-06-04.xlsx'

    sharepoint_dir, download_dir, user_dir = set_folder_defaults()

    file = Path(download_dir) / filename

    with open(file, "rb") as f:
        # reading in the historic excel files
        df = pd.read_excel(f, parse_dates=True)
        f.close()

    # call the main processing
    df = main(df)


# %% [markdown]
# # 5. Development
# 
# Dumping ground for checks that might come in

# %% [markdown]
# ## Non Level 4 ASSY
# 
# Should any part with ASSY in description be at Level 4 only?
# 
# No! this isn't a valid check

# %% [markdown]
# ## Multiple Source Codes
# 
# Check whether a part has been configured with more than one source code within the product structure
# 
# Is it valid to have a TFF part that's FAS and POA?

# %%
def multi_source_code(df):
    unstacked = df.groupby(['Title','Revision','Source Code']).size().unstack()

    # find number of columns dynamically, as number of unique status controls the number of columns
    expected_status_count = len(unstacked.columns) - 1
    unstacked2 = unstacked[unstacked.isna().sum(axis=1)!=expected_status_count]
    unstacked2


    multi_sc = unstacked2.reset_index().fillna('')

    # make_sc_cols = ['AIH','MIH','MOB']

    first_cols = ['Title', 'Revision']

    cols_to_order = first_cols
    sc_ordered_cols = cols_to_order + (multi_sc.columns.drop(cols_to_order).tolist())

    multi_sc = multi_sc[sc_ordered_cols]

    return multi_sc


# %%
def create_heatmap(df, figsize):
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns

       
    hmap = plt.figure(figsize=figsize)
    ax = sns.heatmap(df, annot = True, fmt=".0%", cmap='YlGnBu', annot_kws={'fontsize':8}, linewidths=0.5)
    ax.set(xlabel="", ylabel="")
    ax.xaxis.tick_top()
    plt.rc('xtick', labelsize=10)
    plt.rc('ytick', labelsize=10)
    cbar = ax.collections[0].colorbar
    cbar.set_ticks([0, .2, .75, 1])
    cbar.set_ticklabels(['0%', '20%', '75%', '100%'])
    plt.figure()
    # sns.set(font_scale=.5)
    # plt.show()
    plt.close(hmap)
    return hmap


