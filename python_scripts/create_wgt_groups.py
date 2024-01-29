
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
# Filter data for records from 1982 onwards and for individuals aged 16 and above
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
df = pd.read_parquet(
    f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet",
    columns=columns,
    filters=filters
    )

print("Processing data...")

# Create date variables
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['date_monthly'] = df['date'].dt.to_period('M')

# Set MultiIndex for time series and sort
df.set_index(['personid', 'date_monthly'], inplace=True)
df.sort_index(inplace=True)

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

## Group creation

# Paid hourly group
# 'Hourly' only if true in current year and 12-month lag
df['hrlygroup'] = 'Non-Hourly'
df.loc[(df['paidhrly82'] == 1) & (df['paidhrly82_tm12'] == 1), 'hrlygroup'] = 'Hourly'


# Education groups
df['educgroup'] = np.select(
    [
        df['educ92'].isin([31, 32, 33, 34, 35, 36]), 
        df['educ92'].isin([40, 41, 42, 43])
    ], 
    ['High School or Less', 'College or More'],
    default='Other'
)
print("Education groups created.")

# Gender group
df['gendergroup'] = np.where(df['female76'] == 1, 'Female', 'Male')
print("Gender groups created.")

# Industry groups
industry_conditions = [
    df['industry76'].isin([1, 2]),
    df['industry76'].isin([10, 11]),
    df['industry76'] == 3,
    df['industry76'] == 12,
    df['industry76'].isin([4, 5])
]
industry_choices = ['Business Services', 'Leisure & Hospitality', 'Manufacturing', 'Public Administration', 'Trade & Transportation']
df['indgroup'] = np.select(industry_conditions, industry_choices, default='Other')
print("Industry groups created.")

# Race groups
df['racegroup'] = np.where(df['race76'] == 1, 'White', 'Nonwhite')
print("Race groups created.")

# Metro groups
df['msagroup'] = np.where(df['metstat78'] == 1, 'MSA', 'NonMSA')
print("Metro groups created.")

# Census divisions
census_divisions = {
    1: 'pac', 2: 'esc', 3: 'wsc', 4: 'mnt', 5: 'nen', 
    6: 'sat', 7: 'wnc', 8: 'enc', 9: 'mat'
}
df['cdivgroup'] = df['censusdiv76'].map(census_divisions)
print("Census divisions created.")

# Average wage quartiles
df['wage_hr_avg'] = (df['wageperhrclean82'] + df['wageperhrclean82_lag12']) / 2
df['wage_hr_avg'] = df['wage_hr_avg'].where((df['wagegrowthtracker83'] != '.') | (df['year'].isin([1995, 1996, 1985, 1986])))
for quantile in [25, 50, 75]:
    df[f'p{quantile}_a'] = df.groupby('date_monthly')['wage_hr_avg'].transform(lambda x: x.quantile(quantile / 100.0))
print("Avg wage quartiles created.")

# Allocate observations to wage quartiles
conditions = [
    df['wage_hr_avg'] < df['p25_a'],
    df['wage_hr_avg'] >= df['p25_a'] & df['wage_hr_avg'] < df['p50_a'],
    df['wage_hr_avg'] >= df['p50_a'] & df['wage_hr_avg'] < df['p75_a'],
    df['wage_hr_avg'] >= df['p75_a']
]
choices = ['1st', '2nd', '3rd', '4th']
df['wagegroup'] = np.select(conditions, choices, default=np.nan)
print("Allocated observations to wage quartiles.")

# Filter for individuals aged 16 and above
df = df[df['age76'] >= 16]

# Keep only relevant columns
keep_columns = ['date', 'personid'] + [col for col in df.columns if 'group' in col]
df = df[keep_columns]

# Save the data
df.to_csv(f"{processeddatapath}/WGT_groups.csv", index=False)
print(f"Saved wage growth tracker groups to {processeddatapath}/WGT_groups.csv")
