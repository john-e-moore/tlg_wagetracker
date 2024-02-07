# Wage Gauge
## Overview
From the [Atlanta Fed's Wage Growth Tracker](https://www.atlantafed.org/chcs/wage-growth-tracker) page:
> The Atlanta Fed's Wage Growth Tracker is a measure of the nominal wage growth of individuals. It is constructed 
> using microdata from the [Current Population Survey (CPS)](http://www.census.gov/cps/), and is the median percent 
> change in the hourly wage of individuals observed 12 months apart. Our measure is based on methodology developed
> by [colleagues at the San Francisco Fed](http://www.frbsf.org/economic-research/nominal-wage-rigidity/).

The Atlanta Fed provides Stata resources -- a .dta file for the longitudinally matched microdata, and three .do files for processing the data into what you see on the Wage Growth Tracker page. The resources provided limit users to viewing wage growth based on median percent change in hourly wage of individuals. The code in this repository allows users to create a table with the necessary groups and then run whatever aggregations and analyses they want on top of it.

This project uses Python's Pandas library to write the .dta file contents into a SQLite database table. Once the data is ingested, two .sql scripts are run to process the data into analytics-ready format complete with groups like education level, wage quartile, and more.

## Getting Started
### Prerequisites
A Python installation and SQL client such as DBeaver.

### Installation
##### Clone repository
`git clone https://github.com/john-e-moore/tlg_wagetracker.git`
##### Create and activate virtual environment
1. `cd tlg_wagetracker`
2. `python -m venv venv`
3. `source venv/bin/activate`
##### Install dependencies
`pip install -r requirements.txt`

## Usage
#### Data Download
----------------------------------------
Download the 'Harmonized Variable and Longitudinally Matched [Atlanta Federal Reserve] (1976-Present)' dataset from [The Kansas City Fed's CPS page](https://cps.kansascityfed.org/) and move it to the 'data/' folder inside this project.

The last updated timestamp is included on this page as well as other information about the CPS microdata.

#### SQLite Ingest
----------------------------------------
Running 'ingest/ingest_cps.py' will create a SQLite database (.db file) and a table called 'cps_harmonized_longitudinally_matched'.

#### Data Processing
----------------------------------------
In your SQL client, create a connection to the SQLite database you just created. Run the .sql script in the 'data/sqlite/indexes/' folder to create indexes on 'personid' and 'date_monthly' -- these are the columns you'll be grouping and sorting on.

To create the groups (dimensions to be sliced on) run 'data/sqlite/scripts/create_wgt_groups.sql'. Then, to create the final analysis-ready dataset, run 'data/sqlite/scripts/create_wgt_unweighted.sql'.

## File Structure
Raw data and SQLite-related code is in 'data/'. The raw data ingest script is in  'ingest/'. The Stata scripts provided by the Atlanta Fed are in 'stata_scripts/'.
