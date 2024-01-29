
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype

rawdatapath = "/home/ec2-user/tlg_wagetracker/data"
processeddatapath = "/home/ec2-user/tlg_wagetracker/data"

################################################################################
# Read and filter data
################################################################################
# columns
#paidhrly82* metstat78 censusdiv76 lfdetail94 educ92 race76 employer89 
#occupation76 industry76 sameemployer94 sameactivities94 recession76 
#personid wageperhrclean82* wagegrowthtracker83 date age76 female76 
columns = [
    'paidhrly82', 'metstat78', 'censusdiv76', 'lfdetail94', 'educ92', 'race76',
    'employer89', 'occupation76', 'industry76', 'sameemployer94', 'sameactivities94',
    'recession76', 'personid', 'wageperhrclean82', 'wagegrowthtracker83', 'date',
    'age76', 'female76', 'paidhrly82_tm12'
]
filters = [
    ('date', '>', pd.to_datetime("1982-01-01")),
    ('age76', '>=', 16)
    ]
print("Reading raw data...")
df = pd.read_parquet(
    f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet",
    columns=columns,
    filters=filters
    )

# Create date variables
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['date_monthly'] = df['date'].dt.to_period('M')
print("Date variables created.")

################################################################################
# Optimizations
################################################################################
# Data summary
print(f"Data types:\n{df.dtypes}")
print(f"Memory usage by column:\n{df.memory_usage()}")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")
preview = df.head(50)
preview.to_csv('preview.csv')
print("Preview saved to preview.csv")

# Optimizations
print("Checking for optimizations...")
# All columns are numeric or date, so no categoricals
# Downcast numeric columns
for col in df.columns:
    if is_numeric_dtype(df[col]):
        df[col] = pd.to_numeric(df[col], downcast='integer' or 'float')
        print(f"Downcasted {col}.")
        # Identify float columns that could be integer
        if (df[col].mod(1) == 0).all():
            # Convert to integer and downcast
            df[col] = df[col].astype(int)
            df[col] = pd.to_numeric(df[col], downcast='integer')
    else:
        print(f"{col} is not numeric, skipping.")
print(f"New data types:\n{df.dtypes}")
print(f"New memory usage by column:\n{df.memory_usage()}")
print(f"New overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")

preview = df.head(50)
preview.to_csv('preview2.csv')
print("Preview saved to preview2.csv")

# Save raw data sample
filename = "raw_sample.csv"
raw_sample = df.head(100)
raw_sample.to_csv(f'{rawdatapath}/{filename}', index=False)
print("Raw data sample saved.")

print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")
print("Processing data...")

################################################################################
# Lag creation
################################################################################
df.sort_values(['personid', 'date_monthly'], inplace=True)

# 12-month lags
lag_vars_12 = ['lfdetail94', 'occupation76', 'industry76', 'wageperhrclean82']
for var in lag_vars_12:
    df[var + '_tm12'] = df.groupby('personid')[var].shift(12)
# 1 and 2-month lags
lag_vars_1_and_2 = ['sameemployer94', 'sameactivities94']
for var in lag_vars_1_and_2:
    df[var + '_tm1'] = df.groupby('personid')[var].shift(1)
    df[var + '_tm2'] = df.groupby('personid')[var].shift(2)
print("Lags created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

################################################################################
# Group creation
################################################################################

# Paid hourly group (2)
# 'Hourly' only if true in current year and 12-month lag
df['hrlygroup'] = np.where(
    (df['paidhrly82'] == 1) & (df['paidhrly82_tm12'] == 1), 'Hourly', 'Non-Hourly'
)
print("Paid hourly groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Job leave/stay group (2)
df['jstayergroup'] = 'Job Stayer'
df.loc[
    (df['occupation76'] != df['occupation76_tm12']) | # New occupation
    (df['industry76'] != df['industry76_tm12']) | # New industry
    (df['sameemployer94'] == 2) |
    (df['sameemployer94_tm1'] == 2) |
    (df['sameemployer94_tm2'] == 2) |
    (df['sameactivities94'] == 2) |
    (df['sameactivities94_tm1'] == 2) |
    (df['sameactivities94_tm2'] == 2),
    'jstayergroup'
] = 'Job Switcher'
print("Job leave/stay groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")
# Drop columns only needed to create leave/stay group
df.drop(['sameemployer94_tm1', 'sameemployer94_tm2', 'sameactivities94_tm1', 
         'sameactivities94_tm2', 'industry76_tm12', 'occupation76_tm12', 
         'lfdetail94_tm12'], axis=1, inplace=True)
print("Columns dropped.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Age groups (3)
conditions = [
    df['age76'].between(16, 24, inclusive='both'),
    df['age76'].between(25, 54, inclusive='both'),
    (df['age76'] >= 55) & (~df['age76'].isna())
]
choices = ['16-24', '25-54', '55+']
df['agegroup'] = np.select(conditions, choices, default=pd.NA)
print("Age groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Gender groups (2)
df['gendergroup'] = np.where(df['female76'] == 1, 'Female', 'Male')
print("Gender groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Sector groups (2)
conditions = [
    df['industry76'].isin([1,2,3,13]),
    df['industry76'].isin([4,5,6,7,8,9,10,11,12])
]
choices = ['Goods', 'Services']
df['secgroup'] = np.select(conditions, choices, default=pd.NA)
print("Sector groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Education groups for 12-month avg cuts (3)
conditions = [
    df['educ92'].between(1, 3, inclusive='both'), 
    df['educ92'].between(6, 7, inclusive='both'), 
    df['educ92'].between(4, 5, inclusive='both')
]
choices = ['Nodegree', 'Bachelor+', 'Associates']
df['edgroup3'] = np.select(conditions, choices, default=pd.NA)
print("Education groups (12mo) created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Education groups for 3-month avg cuts (2)
conditions = [
    df['educ92'].between(1, 3, inclusive='both'), 
    df['educ92'].between(4, 7, inclusive='both')
]
choices = ['Nodegree', 'Degree']
df['edgroup2'] = np.select(conditions, choices, default=pd.NA)
print("Education groups (3mo) created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Occupation groups (2)
conditions = [
    df['occupation76'].isin([11,12,13]), 
    df['occupation76'].isin([21,22,23,31,32,33,34])
]
choices = ['Professional', 'Nonprofessional']
df['occgroup'] = np.select(conditions, choices, default=pd.NA)
print("Occupation groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Full-time / part-time groups (2)
conditions = [
    (df['lfdetail94'] == 6) | df['lfdetail94'].between(8, 20, inclusive='both'),
    (df['lfdetail94'] == 7) | df['lfdetail94'].between(21, 32, inclusive='both')
]
choices = ["Full-time", "Part-time"]
df['ftptgroup'] = np.select(conditions, choices, default=pd.NA)
print("Full-time / part-time groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Skill groups (3)
conditions = [
    df['occupation76'].isin([34,32,33]),
    df['occupation76'].isin([21,22,23,31]),
    df['occupation76'].isin([11,12,13])
]
choices = ['Low', 'Middle', 'High']
df['skillgroup'] = np.select(conditions, choices, default=pd.NA)
print("Skill groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Industry groups (7)
conditions = [
    df['industry76'].isin([1, 2]),                              
    df['industry76'] == 9,                                      
    df['industry76'].isin([6, 7, 8]),                           
    df['industry76'].isin([10, 11]),                            
    df['industry76'] == 3,                                      
    df['industry76'] == 12,                                     
    df['industry76'].isin([4, 5])                              
]
choices = [
    "Construction & Mining", "Education & Health", "Finance and Business Services",
    "Leisure & Hospitality", "Manufacturing", "Public Administration", 
    "Trade & Transportation"
]
df['indgroup'] = np.select(conditions, choices, default=pd.NA)
print("Industry groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Race groups (2)
conditions = [
    df['race76'] == 1,
    df['race76'].isin([2,3])
]
choices = ['White', 'Nonwhite']
df['racegroup'] = np.select(conditions, choices, default=pd.NA)
print("Race groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Metro groups (2)
conditions = [
    df['metstat78'] == 1,
    df['metstat78'] == 2
]
df['msagroup'] = np.select(conditions, choices, default=pd.NA)
print("Metro groups created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Census divisions
census_divisions = {
    1: 'pac', 2: 'esc', 3: 'wsc', 4: 'mnt', 5: 'nen', 
    6: 'sat', 7: 'wnc', 8: 'enc', 9: 'mat'
}
df['cdivgroup'] = df['censusdiv76'].map(census_divisions)
print("Census divisions created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Average wage quartiles
condition = (df['wagegrowthtracker83'].notna()) | df['_year'].isin([1995, 1996, 1985, 1986])
df['wage_hr_avg'] = (df['wageperhrclean82'] + df['wageperhrclean82_tm12']) / 2
df.loc[~condition, 'wage_hr_avg'] = pd.NA
for quantile in [25, 50, 75]:
    df[f'p{quantile}_a'] = df.groupby('date_monthly')['wage_hr_avg'].transform(lambda x: x.quantile(quantile / 100.0))
print("Average wage quartiles created.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# Allocate observations to wage quartiles
conditions = [
    df['wage_hr_avg'] < df['p25_a'],
    (df['wage_hr_avg'] >= df['p25_a']) & (df['wage_hr_avg'] < df['p50_a']),
    (df['wage_hr_avg'] >= df['p50_a']) & (df['wage_hr_avg'] < df['p75_a']),
    (df['wage_hr_avg'] >= df['p75_a']) & (df['wage_hr_avg'].notna())
]
choices = ['1st', '2nd', '3rd', '4th']
df['wagegroup'] = np.select(conditions, choices, default=pd.NA)
print("Allocated observations to wage quartiles.")
print(f"Overall memory consumption in GB: {df.memory_usage(deep=True).sum()/(1024**3)}")
print(f"Shape: {df.shape}")

# The original Stata script filters group columns for age 16+ here,
# but we already did that when reading in the data.

# Keep only relevant columns
keep_columns = ['date', 'personid'] + [col for col in df.columns if 'group' in col]
df = df[keep_columns]

################################################################################
# Write output
################################################################################
# Sample
filename = "WGT_groups_sample.csv"
groups_sample = df.head(100)
groups_sample.to_csv(f'{processeddatapath}/{filename}', index=False)
print("Groups sample saved.")
# Full
filename = "WGT_groups.parquet"
df.to_parquet(f"{processeddatapath}/{filename}", index=False)
print(f"Saved wage growth tracker groups to {processeddatapath}/{filename}")