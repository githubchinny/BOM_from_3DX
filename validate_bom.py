# %% [markdown]
# # Validate 
# Author: Mark Chinnock  
# Date: 19/07/2024
# 
# This script can read in an excel file(s) from sharepoint that have previously been extracted from 3dx, processed to add the additional functions/metrics and passed to smartsheets.
# 
# Alternatively, this could read in the 3dx extract directly to validate the latest attributes.
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
        print (key)
        dict_checks[key] = dict_checks[key][check_columns]

    return dict_checks

# %% [markdown]
# ## 1.8 Valid 3dx Dropdown values
# 
# Read in the GMT Standards document from GMT sharepoint folder and confirm 3dx extract contains the same columns and valid values
# 
# 

# %%
def check_attributes(df):
    attr_filename = '3DX Attributes Requirements for Release and Clarification.xlsx'

    attr = pd.read_excel(attr_filename, sheet_name='Drop Down Attributes', na_values="", keep_default_na=False)

    # create a dictionary of all the valid values for each column - this drops the nan values for each column
    attr_d = {attr[column].name: [y for y in attr[column] if not pd.isna(y)] for column in attr}

    for key in attr_d:
        # check the column exists
        try:
            mask = df[key].isin(attr_d[key])
            df[key + ' Check'] = np.where(mask, 'Valid','Invalid')
        except KeyError as e:
            df[key + ' Check'] = 'Not in Extract'

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

# %% [markdown]
# # 3. Write to excel
# 
# Call xlwings with your pre-prepared dictionary and write out many sheets to one excel file, naming the sheets whatever you called your dictionary keys

# %%
def write_to_xl(outfile, df_dict):
    import xlwings as xw
    with xw.App(visible=True) as app:
        try:
            wb = xw.Book(outfile)
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
            if table_name in [table.df for table in ws.tables]:
                ws.tables[table_name].update(df)
            else:
                table_name = ws.tables.add(source=ws['A1'],
                                            name=table_name).update(df)
    wb.save(outfile)

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
if __name__ == '__main__':

    # for reading in multiple files

    # files = find_files()
    dict_df = {}

    filename = 'Updated_T48e-01-Z00001_2024-07-19.xlsx'
    sharepoint_dir, download_dir, user_dir = set_folder_defaults()

    file = Path(download_dir) / filename

    df = pd.DataFrame()

    with open(file, "rb") as f:
        # reading in the historic excel files
        df = pd.read_excel(f, parse_dates=True)
        f.close()

    df.reset_index(drop=False, inplace=True)
    df.rename(columns={'index':'bom_order'}, inplace=True)

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

    # validate the 3dx attributes that have dropdowns
    df = check_attributes(df)

    # write out just the cols we need to report against
    dict_checks = filter_check_columns(dict_checks)

    # add the full df to the sheet
    dict_checks['BOM'] = df



# %% [markdown]
# # 5. Development
# 
# Dumping ground for checks that might come in

# %% [markdown]
# ## Non Level 4 ASSY
# 
# Should any part with ASSY in description be at Level 4 only?

# %%
def Non_level_4_ASSY(df):
    df[df.Description.str.contains('ASSY', na=False)].groupby(['Level']).size()


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


# %% [markdown]
# ### Write out source code checks to excel

# %%
# Write out to excel
pathfile = Path(file.name).stem
output_file = Path(sys.path[0]) / Path(pathfile + '_validated').with_suffix('.xlsx')
# write_to_xl(output_file, dict_checks)

# using inline write to excel as this seems to work better on mac.  
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


