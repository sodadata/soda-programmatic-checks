import csv
import logging
import os
from collections import defaultdict

# Basic logging configuration
logging.basicConfig(level=logging.INFO)


def create_soda_check_files(file_path):
    # Initialize a dictionary to hold table-specific data
    table_data = defaultdict(lambda: {'columns': [], 'schema': 'soda_demo_data_testing'})

    # Open the CSV file
    with open(file_path, mode='r', newline='') as file:
        # Create a CSV dictionary reader
        csv_reader = csv.DictReader(file)

        # Iterate over each row (which will be a dictionary)
        for row in csv_reader:
            table_name = row["table_name"]
            column_name = row["column_name"]
            data_type = row["data_type"]
            schema_name = row["table_schema"]

            # Add column names to the corresponding table
            table_data[table_name]['columns'].append(column_name)
            table_data[table_name]['schema'] = schema_name

    # Generate checks.yaml files for each table
    for table_name, data in table_data.items():
        build_yaml(table_name, data['columns'], data['schema'])


def build_yaml(table_name, columns, schema_name):
    # Prepare the checks YAML content
    checks_content = []

    # Add the checks header for the table name
    checks_content.append(f"checks for {table_name}:")
    checks_content.append("")  # Add a blank line after the header

    # Add schema check
    checks_content.append("  - schema:")
    checks_content.append("      name: Any schema changes")
    checks_content.append("      warn:")
    checks_content.append("        when schema changes:")
    checks_content.append("          - column delete")
    checks_content.append("          - column add")
    checks_content.append("          - column index change")
    checks_content.append("          - column type change")
    checks_content.append("")  # Add a blank line after schema check

    # Add row count check
    checks_content.append("  - row_count > 0")
    checks_content.append("")  # Add a blank line after row count check

    # Add missing value checks for each column
    for column in columns:
        checks_content.append(f"  - missing_count({column}) = 0")
    checks_content.append("")  # Add a blank line after the missing value checks

    # Create directory structure based on schema
    subfolder_path = f"./checks/{schema_name}"
    os.makedirs(subfolder_path, exist_ok=True)

    # Generate the filename without the timestamp
    yaml_file_name = f"{subfolder_path}/{schema_name}.{table_name}.yaml"

    # Save the YAML content to a file, replacing it if it exists
    with open(yaml_file_name, 'w') as yaml_file:
        # Write the content as a plain text to maintain the blank lines and structure
        yaml_file.write("\n".join(checks_content))

    logging.info(f"Generated/Updated {yaml_file_name} with checks for {table_name}")
