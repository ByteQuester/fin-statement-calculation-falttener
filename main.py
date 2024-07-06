import os
import sys

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess
from src.main.python_service.service.sec.SecGov import SecGov
from src.main.python_service.service.services.dataServices import DataServices
from src.main.python_process.XMLProcessor.processXMLFiles import process_xml_files
from src.main.python_process.JSONProcessor.parseJSON import process_calculations_with_hierarchy
from src.main.python_process.NetworkXProcessor.graphX import process_all_tables

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main(start_year: int, end_year: int):
    # Initialize services
    data_services = DataServices()
    index_data_access = IndexDataAccess()

    # Fetch financial data for the given years
    logging.info(f"Fetching financial data for years {start_year} to {end_year}")
    data_services.fetch_ticker_financials_by_year_range(start_year, end_year)

    # Process XML files
    xml_directory = './src/main/python_process/XMLProcessor/fetched_xml'
    output_directory = './src/main/python_process/XMLProcessor/json_lake'
    logging.info("Processing XML files")
    process_xml_files(xml_directory, output_directory)

    # Process JSON files and populate database
    json_directory = './src/main/python_process/XMLProcessor/json_lake'
    logging.info("Processing JSON files and populating database")
    cik_dataframes = process_calculations_with_hierarchy(json_directory)
    index_data_access.create_and_populate_mysql_database(cik_dataframes)

    # Perform NetworkX analysis and update tables
    logging.info("Performing NetworkX analysis and updating tables")
    process_all_tables(index_data_access)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process financial data.')
    parser.add_argument('--start_year', type=int, required=True, help='The start year for fetching financial data')
    parser.add_argument('--end_year', type=int, required=True, help='The end year for fetching financial data')

    args = parser.parse_args()
    main(args.start_year, args.end_year)
