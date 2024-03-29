import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
from sqlalchemy import create_engine

def insert_recent_records_dta_to_sqlite(
    dta_file_path: str, 
    sqlite_db_path: str, 
    table_name: str, 
    chunksize: int
    ):
    """
    Check table for most recent records and only insert new records. 
    The assumption in this approach is that old records are static.
    """
    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Find date of most recent record
    sql = f"SELECT MAX(date) FROM {table_name};"
    cursor.execute(sql)
    result = cursor.fetchone()[0]
    most_recent_record = pd.to_datetime(result)
    print(f"Most recent record: {most_recent_record}")

    # Process chunks
    rows_processed = 0
    for chunk in pd.read_stata(dta_file_path, chunksize=chunksize, convert_categoricals=False):
        # Filter chunk to only new records
        chunk = chunk[chunk['date'] > most_recent_record]
        if chunk.empty:
            print("No new records in chunk, continuing...")
            continue
        else:
            print(f"New records found. Processing {len(chunk)} new records...")
        # Append new records
        chunk.to_sql(table_name, con=conn, if_exists='append', index=False)
        rows_processed += len(chunk)
        print(f"{rows_processed} records processed.")
    # Close connection
    conn.close()

################################################################################
################################################################################

def upsert_dta_to_sqlite(
    dta_file_path: str, 
    sqlite_db_path: str, 
    table_name: str, 
    chunksize: int
    ):
    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    rows_processed = 0
    for chunk in pd.read_stata(dta_file_path, chunksize=chunksize, convert_categoricals=False):
        # Dynamically create column names part and placeholders for VALUES
        columns = ', '.join(chunk.columns)
        placeholders = ', '.join(['?'] * len(chunk.columns))
        # Upsert statement
        upsert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT(obsid) DO UPDATE SET
            {', '.join([f"{col}=excluded.{col}" for col in chunk.columns if col != 'obsid'])};
        """
        # Execute statement for each row
        for row in chunk.itertuples(index=False, name=None):
            cursor.execute(upsert_sql, row)
        # Commit changes
        conn.commit()
        rows_processed += chunksize
        print(f"{rows_processed} rows processed.")
    # Close connection
    conn.close()

################################################################################
################################################################################
    
def dta_to_sqlite(
    dta_file_path: str, 
    sqlite_db_path: str, 
    table_name: str, 
    chunksize: int
    ):
    # Create a database connection
    engine = create_engine(f'sqlite:///{sqlite_db_path}')
    
    # Initialize a variable to track whether the table is created
    table_created = False

    # Read the .dta file in chunks
    # Note: without convert_categoricals=False, read_stata converts numerical columns
    # based on some info inside the .dta file
    rows_processed = 0
    for chunk in pd.read_stata(dta_file_path, chunksize=chunksize, convert_categoricals=False):
        if not table_created:
            # If table doesn't exist, create it
            chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
            table_created = True
        else:
            # Append subsequent chunks to the table
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        rows_processed += chunksize
        print(f"{rows_processed} rows processed.")
    
    print(f"Data has been successfully loaded into the {table_name} table in the SQLite database.")

################################################################################
################################################################################

def parquet_to_sqlite(
    parquet_file_path: str, 
    sqlite_db_path: str, 
    table_name: str
    ):
    # Create a database connection
    engine = create_engine(f'sqlite:///{sqlite_db_path}')

    # Read parquet file
    df = pd.read_parquet(parquet_file_path)
    print("Parquet file read.")

    # Write to SQLite
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Data has been successfully loaded into the {table_name} table in the SQLite database.")

################################################################################
################################################################################

def parquet_to_csv(parquet_file_path: str):
    # Load the Parquet file
    data = pd.read_parquet(parquet_file_path)
    
    # Define the CSV file path (same folder as the Parquet file)
    csv_file_path = parquet_file_path.rsplit('.', 1)[0] + '.csv'
    
    # Save the DataFrame to CSV
    data.to_csv(csv_file_path, index=False)
    
    print(f"Saved CSV file to: {csv_file_path}")

################################################################################
################################################################################

def dta_to_parquet(
    dta_file_path: str, 
    parquet_file_path: str,
    chunksize: int,
    table_schema=None,
    writer=None
    ):
    try:
        for i,chunk in enumerate(pd.read_stata(dta_file_path, chunksize=1000000, convert_categoricals=False)):
            print(f"Processing chunk #{i+1}...")

            table = pa.Table.from_pandas(chunk)
            
            if writer is None:
                # Print column names of first chunk
                for j,colname in enumerate(chunk.columns.tolist()):
                    print(f"({j+1}) {colname}")
                # Initialize the Parquet writer
                writer = pq.ParquetWriter(parquet_file_path, table.schema, compression='snappy')
                table_schema = table.schema
            else:
                # Ensure the chunk's schema matches the file schema
                if table.schema != table_schema:
                    table = table.cast(table_schema)
                    
            writer.write_table(table)
            print(f"{(i+1)*chunksize} rows processed.")
    finally:
        if writer:
            writer.close()

    print("Conversion to Parquet completed.")