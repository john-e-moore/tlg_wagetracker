
import pandas as pd
import numpy as np

# Set file paths (modify as per your directory structure)
#rawdatapath = "/mnt/c/Users/John/Documents/TLG/Wage Growth Tracker/data"  # Path where the raw data is stored
rawdatapath = "/home/john/tlg/wagetracker/data"  # Path where the raw data is stored
#processeddatapath = "/mnt/c/Users/John/Documents/TLG/Wage Growth Tracker/data"  # Path where processed data will be stored
processeddatapath = "/home/john/tlg/wagetracker/data"  # Path where processed data will be stored

# columns
#paidhrly82* metstat78 censusdiv76 lfdetail94 educ92 race76 employer89 
#occupation76 industry76 sameemployer94 sameactivities94 recession76 
#personid wageperhrclean82* wagegrowthtracker83 date age76 female76 

# Read the data (modify file name as per your file)
columns = [
    'paidhrly82', 'metstat78', 'censusdiv76', 'lfdetail94', 'educ92', 'race76',
    'employer89', 'occupation76', 'industry76', 'sameemployer94', 'sameactivities94',
    'recession76', 'personid', 'wageperhrclean82', 'wagegrowthtracker83', 'date',
    'age76', 'female76'
]
filters = [
    ('date', '>', pd.to_datetime("1982-01-01")),
    ('age76', '>', 16)
    ]
print("Reading raw data...")
data = pd.read_parquet(
    f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet",
    columns=columns,
    filters=filters
    )

# Filter data for records from 1982 onwards and for individuals aged 16 and above
print("Processing data...")
#data = data[(data['date'] >= pd.to_datetime("1982-01-01")) & (data['age76'] >= 16)]

# Create date variables
data['year'] = data['date'].dt.year
data['month'] = data['date'].dt.month
data['date_monthly'] = data['date'].dt.to_period('M')

# Group creation
# Lag variables by 12 months
data.sort_values(by=['personid', 'date'], inplace=True)
lag_vars = ['lfdetail94', 'occupation76', 'industry76', 'sameemployer94', 'sameactivities94', 'wageperhrclean82']
for var in lag_vars:
    data[var + '_lag12'] = data.groupby('personid')[var].shift(12)
print("Lags created.")

# Education groups
data['educgroup'] = np.select(
    [
        data['educ92'].isin([31, 32, 33, 34, 35, 36]), 
        data['educ92'].isin([40, 41, 42, 43])
    ], 
    ['High School or Less', 'College or More'],
    default='Other'
)
print("Education groups created.")

# Gender group
data['gendergroup'] = np.where(data['female76'] == 1, 'Female', 'Male')
print("Gender groups created.")

# Industry groups
industry_conditions = [
    data['industry76'].isin([1, 2]),
    data['industry76'].isin([10, 11]),
    data['industry76'] == 3,
    data['industry76'] == 12,
    data['industry76'].isin([4, 5])
]
industry_choices = ['Business Services', 'Leisure & Hospitality', 'Manufacturing', 'Public Administration', 'Trade & Transportation']
data['indgroup'] = np.select(industry_conditions, industry_choices, default='Other')
print("Industry groups created.")

# Race groups
data['racegroup'] = np.where(data['race76'] == 1, 'White', 'Nonwhite')
print("Race groups created.")

# Metro groups
data['msagroup'] = np.where(data['metstat78'] == 1, 'MSA', 'NonMSA')
print("Metro groups created.")

# Census divisions
census_divisions = {
    1: 'pac', 2: 'esc', 3: 'wsc', 4: 'mnt', 5: 'nen', 
    6: 'sat', 7: 'wnc', 8: 'enc', 9: 'mat'
}
data['cdivgroup'] = data['censusdiv76'].map(census_divisions)
print("Census divisions created.")

# Average wage quartiles
data['wage_hr_avg'] = (data['wageperhrclean82'] + data['wageperhrclean82_lag12']) / 2
data['wage_hr_avg'] = data['wage_hr_avg'].where((data['wagegrowthtracker83'] != '.') | (data['year'].isin([1995, 1996, 1985, 1986])))
for quantile in [25, 50, 75]:
    data[f'p{quantile}_a'] = data.groupby('date_monthly')['wage_hr_avg'].transform(lambda x: x.quantile(quantile / 100.0))
print("Avg wage quartiles created.")

# Allocate observations to wage quartiles
conditions = [
    data['wage_hr_avg'] < data['p25_a'],
    data['wage_hr_avg'] >= data['p25_a'] & data['wage_hr_avg'] < data['p50_a'],
    data['wage_hr_avg'] >= data['p50_a'] & data['wage_hr_avg'] < data['p75_a'],
    data['wage_hr_avg'] >= data['p75_a']
]
choices = ['1st', '2nd', '3rd', '4th']
data['wagegroup'] = np.select(conditions, choices, default=np.nan)
print("Allocated observations to wage quartiles.")

# Filter for individuals aged 16 and above
data = data[data['age76'] >= 16]

# Keep only relevant columns
keep_columns = ['date', 'personid'] + [col for col in data.columns if 'group' in col]
data = data[keep_columns]

# Save the data
data.to_csv(f"{processeddatapath}/WGT_groups.csv", index=False)
print(f"Saved wage growth tracker groups to {processeddatapath}/WGT_groups.csv")