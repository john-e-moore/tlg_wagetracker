import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define the path to your .dta file
dta_file_path = '/home/john/tlg/wagetracker/data/CPS_harmonized_variable_longitudinally_matched_age16plus.dta'
# Define the path for the output .parquet file
parquet_file_path = '/home/john/tlg/wagetracker/data/CPS_harmonized_variable_longitudinally_matched_age16plus.parquet'

chunk_size = 100000  # Adjust based on your system's memory capacity
print(f"Chunk size = {chunk_size}")
table_schema = None  # To store the schema of our Parquet file

writer = None
try:
    for i,chunk in enumerate(pd.read_stata(dta_file_path, chunksize=chunk_size, convert_categoricals=False)):
        print(f"Processing chunk #{i+1}")

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
finally:
    if writer:
        writer.close()

print("Conversion to Parquet completed.")
