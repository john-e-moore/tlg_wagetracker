import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from ingest_utils import read_dta_write_sqlite

if __name__ == "__main__":
    dta_file_path = '/home/john/tlg/wagetracker/data/CPS_harmonized_variable_longitudinally_matched_age16plus.dta'
    sqlite_db_path = '/home/john/tlg/wagetracker/data/tlg.db'
    table_name = 'cps_harmonized_longitudinally_matched'
    chunksize = 1000000

    read_dta_write_sqlite(dta_file_path, sqlite_db_path, table_name, chunksize)
