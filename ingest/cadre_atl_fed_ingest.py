import pandas as pd
import sqlite3
from sqlalchemy import create_engine

def read_dta_write_sqlite(dta_file_path, sqlite_db_path, table_name, chunksize):
    # Create a database connection
    engine = create_engine(f'sqlite:///{sqlite_db_path}')
    
    # Initialize a variable to track whether the table is created
    table_created = False

    # Read the .dta file in chunks
    # Note: without convert_categoricals=False, read_stata converts numerical columns
    # based on some info inside the .dta file
    chunks_processed = 0
    for chunk in pd.read_stata(dta_file_path, chunksize=chunksize, convert_categoricals=False):
        if not table_created:
            print(chunk.describe())
            chunk.describe().to_csv('chunk_describe.csv')
            chunk.head(100).to_csv('chunk_preview.csv')
            # If table doesn't exist, create it
            chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
            table_created = True
        else:
            # Append subsequent chunks to the table
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        chunks_processed += chunksize
        print(f"{chunks_processed} processed.")
    
    print(f"Data has been successfully loaded into the {table_name} table in the SQLite database.")

def read_parquet_write_sqlite(parquet_file_path, sqlite_db_path, table_name):
    # Create a database connection
    engine = create_engine(f'sqlite:///{sqlite_db_path}')

    # Read parquet file
    df = pd.read_parquet(parquet_file_path)
    print("Parquet file read.")

    # Write to SQLite
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Data has been successfully loaded into the {table_name} table in the SQLite database.")

if __name__ == "__main__":
    dta_file_path = '/home/john/tlg/wagetracker/data/CPS_harmonized_variable_longitudinally_matched_age16plus.dta'
    sqlite_db_path = '/home/john/tlg/wagetracker/data/tlg.db'
    table_name = 'cps_harmonized_longitudinally_matched'
    chunksize = 1000000

    read_dta_write_sqlite(dta_file_path, sqlite_db_path, table_name, chunksize)

    #parquet_file_path = '/home/john/tlg/wagetracker/data/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet'
    #read_parquet_write_sqlite(parquet_file_path, sqlite_db_path, table_name)
