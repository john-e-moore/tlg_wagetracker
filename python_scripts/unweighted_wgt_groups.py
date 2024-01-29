
import pandas as pd
import numpy as np

# Set file paths (modify as per your directory structure)
rawdatapath = "C:/WageGrowthTracker/Data/rawdata"  # Path where the raw data is stored
processeddatapath = "C:/WageGrowthTracker/Data/processeddata"  # Path where processed data will be stored

################################################################################
# Read and merge data
################################################################################
# Read Cadre data
print("Reading Cadre data...")
columns = [
    'date', 'personid', 'recession76', 'age76', 
    'wageperhrclean82', 'wagegrowthtracker83'
]
filters = [
    ('date', '>', pd.to_datetime("1982-01-01")),
    ('age76', '>=', 16)
    ]
cadre_df = pd.read_parquet(
    f"{rawdatapath}/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet",
    columns=columns,
    filters=filters
)
cadre_df.rename(columns={'wagegrowthtracker83': 'wgt'}, inplace=True)

# Read groups created by create_wgt_groups.py
print("Reading WGT groups data...")
wgt_groups_df = pd.read_parquet(f"{rawdatapath}/WGT_groups.parquet")

# Merge the datasets
print("Merging datasets...")
df = cadre_df.merge(wgt_groups_df, on=['personid', 'date'], how='left')
print("Merge successful.")

# Create date variables
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['date_monthly'] = df['date'].dt.to_period('M')

################################################################################
# Create unweighted time series variables
################################################################################
# Create number of zero wage changes
conditions = [
    df['wgt'].isna(),
    df['wgt'].abs() < 0.5
]
choices = [0, 100]
df['wgt_zer'] = np.select(conditions, choices, default=pd.NA)

# Drop missing wgt observations from dataset except for 85-86 and 95-96 when ALL wgt
# observations are missing for some months due to Census masking of identifiers
# (need to keep those missing months for collapsed dataset)
df = df[df['wgt'].notna() | df['year'].isin([1985, 1986, 1995, 1996])]

# Create variables for unsmoothed version
# For unsmoothed version
df['wgt_raw'] = df['wgt']

# Average wage quartiles
df['wgt_q1'] = df['wgt'].where(df['wagegroup'] == '1st')
df['wgt_q2'] = df['wgt'].where(df['wagegroup'] == '2nd')
df['wgt_q3'] = df['wgt'].where(df['wagegroup'] == '3rd')
df['wgt_q4'] = df['wgt'].where(df['wagegroup'] == '4th')

# Metro and non-metro
df['wgt_ym'] = df['wgt'].where(df['msagroup'] == 'MSA')
df['wgt_nm'] = df['wgt'].where(df['msagroup'] == 'NonMSA')

# 3 age groups
df['wgt_ya'] = df['wgt'].where(df['agegroup'] == '16-24')
df['wgt_pa'] = df['wgt'].where(df['agegroup'] == '25-54')
df['wgt_oa'] = df['wgt'].where(df['agegroup'] == '55+')

# Usually ft/usually pt
df['wgt_ft'] = df['wgt'].where(df['ftptgroup'] == 'Full-time')
df['wgt_pt'] = df['wgt'].where(df['ftptgroup'] == 'Part-time')

# Male/female
df['wgt_ms'] = df['wgt'].where(df['gengroup'] == 'Male')
df['wgt_ws'] = df['wgt'].where(df['gengroup'] == 'Female')

# Degree education
df['wgt_he'] = df['wgt'].where(df['edgroup3'].isin(['Bachelor+', 'Associates']))

# Three ed groups
df['wgt_de'] = df['wgt'].where(df['edgroup3'] == 'Bachelor+')
df['wgt_ae'] = df['wgt'].where(df['edgroup3'] == 'Associates')
df['wgt_le'] = df['wgt'].where(df['edgroup3'] == 'Nodegree')

# Skill
df['wgt_lo'] = df['wgt'].where(df['skillgroup'] == 'Low')
df['wgt_mo'] = df['wgt'].where(df['skillgroup'] == 'Middle')
df['wgt_ho'] = df['wgt'].where(df['skillgroup'] == 'High')

# Service and goods industries
df['wgt_si'] = df['wgt'].where(df['secgroup'] == 'Services')
df['wgt_gi'] = df['wgt'].where(df['secgroup'] == 'Goods')

# White and other race
df['wgt_wr'] = df['wgt'].where(df['racegroup'] == 'White')
df['wgt_or'] = df['wgt'].where(df['racegroup'] == 'Nonwhite')

# Job stayer/switcher
df['wgt_jst'] = df['wgt'].where(df['jstayergroup'] == 'Job Stayer')
df['wgt_jsw'] = df['wgt'].where(df['jstayergroup'] == 'Job Switcher')

# Census division
census_divisions = ['pac', 'esc', 'wsc', 'mnt', 'nen', 'sat', 'wnc', 'enc', 'mat']
for div in census_divisions:
    df[f'wgt_{div}'] = df['wgt'].where(df['cdivgroup'] == div)

# Industries
industries = {
    'cmi': 'Construction & Mining',
    'ehi': 'Education & Health',
    'fpi': 'Finance and Business Services',
    'lhi': 'Leisure & Hospitality',
    'mni': 'Manufacturing',
    'pai': 'Public Administration',
    'tti': 'Trade & Transportation'
}
for suffix, group in industries.items():
    df[f'wgt_{suffix}'] = df['wgt'].where(df['indgroup'] == group)

# Hourly
df['wgt_yhr'] = df['wgt'].where(df['hrlygroup'] == 'Hourly')
df['wgt_nhr'] = df['wgt'].where(df['hrlygroup'] == 'Non-Hourly')

print("Time series variables created.")

# Filter, then save unweighted individual level wgt observations for various cuts
columns_to_keep = ['personid', 'year', 'month', 'date_monthly', 'recession76'] + \
                  [col for col in df.columns if col.startswith('wgt') or 'group' in col]
df = df[columns_to_keep]
# Sample
filename = "wage-growth-data_unweighted_sample.csv"
sample = df.head(100)
sample.to_csv(f'{processeddatapath}/{filename}', index=False)
print("Sample saved.")
# Full
filename = "wage-growth-data_unweighted.parquet"
df.to_parquet(f"{processeddatapath}/{filename}", index=False)
print(f"Saved unweighted wage growth data to {processeddatapath}/{filename}")

################################################################################
# Collapse dataset into time series
################################################################################
# Define aggregations for each column
aggregations = {
    'wgt': 'median',
    'wgt_raw': 'median',
    'wgt_ya': 'median',
    'wgt_pa': 'median',
    'wgt_oa': 'median',
    'wgt_ft': 'median',
    'wgt_pt': 'median',
    'wgt_ms': 'median',
    'wgt_ws': 'median',
    'wgt_he': 'median',
    'wgt_de': 'median',
    'wgt_ae': 'median',
    'wgt_le': 'median',
    'wgt_si': 'median',
    'wgt_gi': 'median',
    'wgt_wr': 'median',
    'wgt_or': 'median',
    'wgt_ho': 'median',
    'wgt_lo': 'median',
    'wgt_mo': 'median',
    'wgt_jst': 'median',
    'wgt_jsw': 'median',
    'wgt_cmi': 'median',
    'wgt_ehi': 'median',
    'wgt_fpi': 'median',
    'wgt_lhi': 'median',
    'wgt_mni': 'median',
    'wgt_pai': 'median',
    'wgt_tti': 'median',
    'wgt_pac': 'median',
    'wgt_esc': 'median',
    'wgt_wsc': 'median',
    'wgt_mnt': 'median',
    'wgt_nen': 'median',
    'wgt_sat': 'median',
    'wgt_wnc': 'median',
    'wgt_enc': 'median',
    'wgt_mat': 'median',
    'wgt_ym': 'median',
    'wgt_nm': 'median',
    'wgt_q1': 'median',
    'wgt_q2': 'median',
    'wgt_q3': 'median',
    'wgt_q4': 'median',
    'wgt_yhr': 'median',
    'wgt_nhr': 'median',
    'wgt_avg': ('wgt', 'mean'),
    'zero': ('wgt_zer', 'mean'),
    'rec': ('recession76', 'mean'),
    'wgt_p25': ('wgt', lambda x: x.quantile(0.25)),
    'wgt_p75': ('wgt', lambda x: x.quantile(0.75)),
    'wgt_n': ('wgt', 'count'),
    'wgt_jst_n': ('wgt_jst', 'count'),
    'wgt_jsw_n': ('wgt_jsw', 'count'),
    'wgt_ft_n': ('wgt_ft', 'count'),
    'wgt_pt_n': ('wgt_pt', 'count'),
    'wgt_he_n': ('wgt_he', 'count'),
    'wgt_si_n': ('wgt_si', 'count'),
    'wgt_pa_n': ('wgt_pa', 'count'),
    'wgt_ws_n': ('wgt_ws', 'count'),
    'wgt_ms_n': ('wgt_ms', 'count')
}

# Collapse the DataFrame
print("Performing aggregations...")
collapsed_df = df.groupby(['date_monthly', 'year', 'month']).agg(aggregations)
print("Aggregations done.")

# Reset the index to flatten the DataFrame structure
collapsed_df.reset_index(inplace=True)

# Create and format date column
collapsed_df['date'] = pd.to_datetime({'year': collapsed_df['year'], 'month': collapsed_df['month'], 'day': 1})
collapsed_df['date'] = collapsed_df['date'].dt.strftime('%m/%d/%Y')

# Sample
filename = "wage-growth-data_unweighted_collapsed_sample.csv"
sample = collapsed_df.head(100)
sample.to_csv(f'{processeddatapath}/{filename}', index=False)
print("Sample saved.")
# Full
filename = "wage-growth-data_unweighted_collapsed.parquet"
collapsed_df.to_parquet(f"{processeddatapath}/{filename}", index=False)
print(f"Saved unsmoothed unweighted cuts to {processeddatapath}/{filename}")

################################################################################
# Create smoothed versions of unweighted wgt time series rounded to 1 decimal place
################################################################################
# 3mma overall series from 1983 and 3mma and 12mma cuts from 1997
collapsed_df = collapsed_df[collapsed_df['year'] >= 1983]

# Set time series index
collapsed_df['date_monthly'] = pd.to_datetime(collapsed_df['date_monthly'])
collapsed_df.set_index('date_monthly', inplace=True)

# Variables to process
variables = ['wgt', 'wgt_ym', 'wgt_nm', 'wgt_gi', 'wgt_si', 'wgt_ft', 'wgt_pt', 'wgt_de', 'wgt_he', 'wgt_le', 'wgt_ae', 
             'wgt_ya', 'wgt_oa', 'wgt_pa', 'wgt_ws', 'wgt_ms', 'wgt_jst', 'wgt_jsw', 'wgt_wr', 'wgt_or', 'wgt_cmi', 
             'wgt_ehi', 'wgt_fpi', 'wgt_lhi', 'wgt_mni', 'wgt_pai', 'wgt_tti', 'wgt_ho', 'wgt_lo', 'wgt_mo', 'wgt_nen', 
             'wgt_mat', 'wgt_enc', 'wgt_wnc', 'wgt_sat', 'wgt_esc', 'wgt_wsc', 'wgt_mnt', 'wgt_pac', 'wgt_avg', 'wgt_p25', 
             'wgt_p75', 'zero', 'wgt_q1', 'wgt_q2', 'wgt_q3', 'wgt_q4']

# Calculate moving averages
for var in variables:
    # Calculate 3-month moving average
    collapsed_df[f'{var}_3mma'] = collapsed_df[var].rolling(window=3, min_periods=1).mean()
    # Calculate 12-month moving average
    collapsed_df[f'{var}_12mma'] = collapsed_df[var].rolling(window=12, min_periods=1).mean()

# Define special conditions for exclusion
special_conditions_3mma = [
    (collapsed_df['year'] == 1995) & (collapsed_df['month'].isin([6, 7])),
    (collapsed_df['year'] == 1996) & (collapsed_df['month'].isin([9, 10])),
    (collapsed_df['year'] == 1985) & (collapsed_df['month'].isin([7, 8])),
    (collapsed_df['year'] == 1986) & (collapsed_df['month'].isin([10, 11]))
]
special_conditions_12mma = [
    (collapsed_df['year'] == 1995) & (collapsed_df['month'].between(6, 12, inclusive='both')),
    (collapsed_df['year'] == 1996),
    (collapsed_df['year'] == 1997) & (collapsed_df['month'].between(1, 7, inclusive='both')),
    (collapsed_df['year'] == 1985) & (collapsed_df['month'].between(7, 12, inclusive='both')),
    (collapsed_df['year'] == 1986),
    (collapsed_df['year'] == 1987) & (collapsed_df['month'].between(1, 8, inclusive='both'))
]

# Change to null if special conditions are met
for condition in special_conditions_3mma:
    for var in variables:
        collapsed_df.loc[condition, f'{var}_3mma'] = pd.NA

for condition in special_conditions_12mma:
    for var in variables:
        collapsed_df.loc[condition, f'{var}_12mma'] = pd.NA

columns_to_keep = ['date', 'year', 'month', 'date_monthly', 'wgt_raw'] + \
                  [col for col in collapsed_df.columns if '3mma' in col or '12mma' in col] + \
                  ['rec']
result = collapsed_df[columns_to_keep]

# Rounding to one decimal place
#df_grouped = df_grouped.round(1)

# Sample
filename = "wage-growth-data_unweighted_smoothed_sample.csv"
sample = result.head(100)
sample.to_csv(f'{processeddatapath}/{filename}', index=False)
print("Sample saved.")
# Full
filename = "wage-growth-data_unweighted_smoothed.parquet"
result.to_parquet(f"{processeddatapath}/{filename}", index=False)
print(f"Saved smoothed unweighted cuts to {processeddatapath}/{filename}")
