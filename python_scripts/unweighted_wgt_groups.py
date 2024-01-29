
import pandas as pd
import numpy as np

# Set file paths (modify as per your directory structure)
rawdatapath = "C:/WageGrowthTracker/Data/rawdata"  # Path where the raw data is stored
processeddatapath = "C:/WageGrowthTracker/Data/processeddata"  # Path where processed data will be stored

# Read the data (modify file name as per your file)
df = pd.read_stata(f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.dta")

# Filter data for records from 1982 onwards and for individuals aged 16 and above
df = df[(df['date'] >= pd.to_datetime("1982-01-01")) & (df['age76'] >= 16)]

# Merge with WGT group data (assuming the file exists and has been created by Create_WGT_groups_usingcadre.py)
wgt_groups = pd.read_stata(f"{rawdatapath}/WGT_groups.dta")
df = df.merge(wgt_groups, on=['personid', 'date'], how='left')

# Create date variables
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['date_monthly'] = df['date'].dt.to_period('M')

# Collapsing data to monthly level
group_vars = [col for col in df.columns if 'group' in col]
df_grouped = df.groupby(['date_monthly'] + group_vars)['wagegrowthtracker83'].median().reset_index()

# Creating smoothed time series using 3-month and 12-month moving averages
for col in df_grouped.columns:
    if col not in ['date_monthly'] + group_vars:
        df_grouped[col+'_3mma'] = df_grouped[col].rolling(window=3, min_periods=1).mean()
        df_grouped[col+'_12mma'] = df_grouped[col].rolling(window=12, min_periods=1).mean()

# Rounding to one decimal place
df_grouped = df_grouped.round(1)

# Extracting year and month from date_monthly for saving
df_grouped['year'] = df_grouped['date_monthly'].dt.year
df_grouped['month'] = df_grouped['date_monthly'].dt.month

# Save the data
df_grouped.to_csv(f"{processeddatapath}/wage-growth-data_unweighted_smoothed.csv", index=False)
