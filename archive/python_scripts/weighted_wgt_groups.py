
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

# Function to create weighted WGT time series
def create_weighted_wgt(data, weight_column, output_name):
    # Calculate weighted median
    data_grouped = data.groupby('date_monthly').apply(lambda x: np.average(x['wagegrowthtracker83'], weights=x[weight_column])).reset_index()
    data_grouped.columns = ['date_monthly', output_name]

    # Creating smoothed time series using 3-month and 12-month moving averages
    data_grouped[output_name+'_3mma'] = data_grouped[output_name].rolling(window=3, min_periods=1).mean()
    data_grouped[output_name+'_12mma'] = data_grouped[output_name].rolling(window=12, min_periods=1).mean()

    # Rounding to one decimal place
    data_grouped = data_grouped.round(1)

    # Extracting year and month from date_monthly for saving
    data_grouped['year'] = data_grouped['date_monthly'].dt.year
    data_grouped['month'] = data_grouped['date_monthly'].dt.month

    # Save the data
    data_grouped.to_csv(f"{processeddatapath}/wage-growth-data_{output_name}_smoothed.csv", index=False)

# Run the function for 'weightern82' and 'weight_97_demojob' (assuming these columns exist)
create_weighted_wgt(data, 'weightern82', 'weighted')
create_weighted_wgt(data, 'weight_97_demojob', 'weighted_97')
