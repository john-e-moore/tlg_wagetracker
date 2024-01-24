
import pandas as pd
import numpy as np

# Set file paths (modify as per your directory structure)
rawdatapath = "C:/WageGrowthTracker/Data/rawdata"  # Path where the raw data is stored
processeddatapath = "C:/WageGrowthTracker/Data/processeddata"  # Path where processed data will be stored

# Read the data (modify file name as per your file)
data = pd.read_stata(f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.dta")

# Filter data for records from 1982 onwards and for individuals aged 16 and above
data = data[(data['date'] >= pd.to_datetime("1982-01-01")) & (data['age76'] >= 16)]

# Merge with WGT group data (assuming the file exists and has been created by Create_WGT_groups_usingcadre.py)
wgt_groups = pd.read_stata(f"{rawdatapath}/WGT_groups.dta")
data = data.merge(wgt_groups, on=['personid', 'date'], how='left')

# Create date variables
data['year'] = data['date'].dt.year
data['month'] = data['date'].dt.month
data['date_monthly'] = data['date'].dt.to_period('M')

# Collapsing data to monthly level
group_vars = [col for col in data.columns if 'group' in col]
data_grouped = data.groupby(['date_monthly'] + group_vars)['wagegrowthtracker83'].median().reset_index()

# Creating smoothed time series using 3-month and 12-month moving averages
for col in data_grouped.columns:
    if col not in ['date_monthly'] + group_vars:
        data_grouped[col+'_3mma'] = data_grouped[col].rolling(window=3, min_periods=1).mean()
        data_grouped[col+'_12mma'] = data_grouped[col].rolling(window=12, min_periods=1).mean()

# Rounding to one decimal place
data_grouped = data_grouped.round(1)

# Extracting year and month from date_monthly for saving
data_grouped['year'] = data_grouped['date_monthly'].dt.year
data_grouped['month'] = data_grouped['date_monthly'].dt.month

# Save the data
data_grouped.to_csv(f"{processeddatapath}/wage-growth-data_unweighted_smoothed.csv", index=False)
