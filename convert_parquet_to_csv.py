import pandas as pd

def parquet_to_csv(parquet_file_path):
    # Load the Parquet file
    data = pd.read_parquet(parquet_file_path)
    
    # Define the CSV file path (same folder as the Parquet file)
    csv_file_path = parquet_file_path.rsplit('.', 1)[0] + '.csv'
    
    # Save the DataFrame to CSV
    data.to_csv(csv_file_path, index=False)
    
    print(f"Saved CSV file to: {csv_file_path}")

# Example usage
parquet_file_path = '/home/john/tlg/wagetracker/data/wage-growth-data_unweighted_smoothed.parquet'
parquet_to_csv(parquet_file_path)

parquet_file_path = '/home/john/tlg/wagetracker/data/wage-growth-data_weighted_smoothed.parquet'
parquet_to_csv(parquet_file_path)
