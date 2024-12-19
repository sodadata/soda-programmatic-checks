from soda.scan import Scan
from datetime import datetime

# TODO:
# - add support for partitioning


def run_soda_scan(data_source_name,config_folder,checks_folder,local):
    # Setup new Soda scan
    scan = Scan()

    # Setup data source
    scan.set_data_source_name(data_source_name)
    scan.add_configuration_yaml_files(config_folder)

    # Setup scan definition
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    scan.set_scan_definition_name("Soda Scaler Run - " + formatted_time)

    # Add checks
    scan.add_sodacl_yaml_files(checks_folder)

    # Configure local scan for debugging
    scan.set_is_local(local)

    # Execute Soda scan
    scan.execute()

    # Get scan results
    results = scan.get_scan_results()

    return results





