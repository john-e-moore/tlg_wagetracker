import yaml
from ingest_utils import dta_to_sqlite

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

if __name__ == "__main__":
    # Load config
    config_path = 'path_to_config.yml'
    config = load_config(config_path)

    dta_file_path = config['dta_file_path']
    sqlite_file_path = config['sqlite_file_path']
    table_name = config['table_name']
    chunksize = config['chunksize']

    # Run ingest
    dta_to_sqlite(dta_file_path, sqlite_file_path, table_name, chunksize)
