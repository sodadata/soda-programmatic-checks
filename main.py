import logging
import os
import glob
from helpers import soda_scan
from helpers import yaml_creator
from helpers import fetch_schemas
import time


#TODO
# add variables to scaler config

# Basic logging configuration
logging.basicConfig(level=logging.INFO)

def process_csv_files(folder_path, process_method):
    # Get a list of all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

    # Loop over each CSV file
    for csv_file in csv_files:
        # Call the provided method on the file path
        process_method(csv_file)

def execute_checks_method(checks_folder):
    # Iterate through each subfolder in 'checks'
    for schema_name in os.listdir(checks_folder):
        schema_folder = os.path.join(checks_folder, schema_name)

        # Only process if it's a directory (schema folder)
        if os.path.isdir(schema_folder):
            logging.info("Running Soda scans for data source: " + schema_name)
            # Call your method here with the data source name (schema name) and the schema folder path
            soda_scan.run_soda_scan(data_source_name=schema_name.lower(), config_folder="./configs/soda_library_configs",
                                    checks_folder=schema_folder, local=False)

if __name__ == '__main__':
    start_time = time.time()  # Record the start time

    logging.info("STARTING SODA SCALER")

    # Get schemas from DB
    config_folder_path = './configs/soda_library_configs'  # Path to the folder containing YAML config files
    fetch_schemas.generate_schema_files(config_folder_path)

    # Build check files
    process_csv_files(folder_path="./input_schemas",process_method=yaml_creator.create_soda_check_files)

    # Run scans
    checks_folder = './checks'  # Provide the full path to the 'checks' folder here
    execute_checks_method(checks_folder)

    logging.info("SODA SCALER ENDED!")

    end_time = time.time()  # Record the end time
    run_time = end_time - start_time  # Calculate the difference

    # Check if run time is greater than or equal to 1 minute
    if run_time >= 60:
        minutes = run_time // 60
        seconds = run_time % 60
        logging.info(f"Soda Scaler took {int(minutes)} minute(s) and {seconds:.4f} second(s) to run.")
    else:
        logging.info(f"Soda Scaler took {run_time:.4f} seconds to run.")



