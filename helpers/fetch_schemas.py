import logging
import yaml
import psycopg2  # For PostgreSQL and Redshift
import pyodbc  # For SQL Server
import snowflake.connector  # For Snowflake
from google.cloud import bigquery  # For BigQuery
import databricks.sql  # For Databricks
import csv  # For writing CSV files
import os  # For interacting with the file system
import boto3  # For AWS connection and assuming role
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Basic logging configuration
logging.basicConfig(level=logging.INFO)

# Function to load configuration from a YAML file
def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Function to connect based on database type
def connect_to_database(config, db_type):
    connection_details = config['connection'] if 'connection' in config else config
    if db_type == 'postgres' or db_type == 'redshift':
        # For Redshift, the port is excluded
        connection_params = {
            'host': connection_details['host'],
            'user': connection_details['username'],
            'password': connection_details['password'],
            'dbname': connection_details['database']
        }
        if db_type == 'postgres':
            connection_params['port'] = connection_details['port']

        if db_type == 'redshift':
            connection_params['port'] = '5439'

        return psycopg2.connect(**connection_params)
    elif db_type == 'sqlserver':
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={connection_details['host']};"
            f"DATABASE={connection_details['database']};"
            f"UID={connection_details['username']};"
            f"PWD={connection_details['password']};"
            f"Encrypt={connection_details.get('encrypt', 'false')};"
            f"TrustServerCertificate={connection_details.get('trust_server_certificate', 'false')};"
        )
        return pyodbc.connect(connection_string)
    elif db_type == 'snowflake':
        return snowflake.connector.connect(
            user=connection_details['username'],
            password=connection_details['password'],
            account=connection_details['account'],
            warehouse=connection_details['warehouse'],
            database=connection_details['database'],
            schema=config['schema'],
            role=connection_details.get('role', None),
            session_parameters=connection_details.get('session_parameters', {})
        )
    elif db_type == 'bigquery':
        return bigquery.Client.from_service_account_json(
            connection_details['account_info_json'],
            project=connection_details['project_id']
        )
    elif db_type == 'spark':
        return databricks.sql.connect(
            server_hostname=connection_details['host'],
            http_path=connection_details['http_path'],
            access_token=connection_details['token'],
            catalog=connection_details['catalog']
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

# Function to fetch information schema based on the database type
def fetch_information_schema(connection, db_type, schema_name, output_file, catalog=None):
    if db_type == 'postgres' or db_type == 'redshift':
        query = f"""
        SELECT table_name, table_schema, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        """
    elif db_type == 'sqlserver':
        query = f"""
        SELECT table_name, table_schema, column_name, data_type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema_name}'
        """
    elif db_type == 'snowflake':
        query = f"""
        SELECT table_name, table_schema, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        """
    elif db_type == 'bigquery':
        query = f"""
        SELECT table_name, table_schema, column_name, data_type
        FROM `{schema_name}.INFORMATION_SCHEMA.COLUMNS`
        """
    elif db_type == 'spark':
        if not catalog:
            raise ValueError("Catalog is required for Spark connections.")
        query = f"""
        SELECT table_name, table_schema, column_name, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema_name}'
        """
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    cursor = connection.cursor()
    cursor.execute(query)
    columns = cursor.fetchall()

    # Write results to CSV in the input_schemas folder
    if not os.path.exists('input_schemas'):
        os.makedirs('input_schemas')  # Create the folder if it doesn't exist

    output_file_path = os.path.join('input_schemas', output_file)

    with open(output_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["table_name", "table_schema", "column_name", "data_type"])  # Write header
        for column in columns:
            csvwriter.writerow([column[0], column[1], column[2], column[3]])  # Write data

    cursor.close()

# Main function to execute the script
def generate_schema_files(config_folder_path):
    # Iterate through each YAML file in the folder
    for config_file in os.listdir(config_folder_path):
        if config_file.endswith(".yaml"):
            config_file_path = os.path.join(config_folder_path, config_file)
            logging.info(f"Processing config file: {config_file_path}")

            config = load_config(config_file_path)

            # Iterate through all keys and look for the ones with type and schema
            for data_source_key, data_source_value in config.items():
                if isinstance(data_source_value, dict) and 'type' in data_source_value and 'schema' in data_source_value:
                    db_type = data_source_value['type']
                    schema_name = data_source_value['schema']
                    catalog = data_source_value.get('catalog', 'unity_catalog')

                    # Connect to the correct database based on the type
                    connection = connect_to_database(data_source_value, db_type)

                    # Generate a CSV file for each schema (named based on the schema name or config file name)
                    output_file = f"{schema_name}_schema_information.csv"
                    if connection:
                        fetch_information_schema(connection, db_type, schema_name, output_file, catalog)