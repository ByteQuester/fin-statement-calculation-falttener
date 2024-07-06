import os
import concurrent.futures
from . import parseXML

def process_xml_files(directory, output_directory, arelle_path='src/main/python_process/Arelle-master/arelleCmdLine.py', python_path='/usr/bin/python3'):
    os.makedirs(output_directory, exist_ok=True)
    xml_files = parseXML.get_xml_files(directory)
    args_list = [(xml_file, output_directory, arelle_path, python_path) for xml_file in xml_files]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(parseXML.process_xml_file, args) for args in args_list]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")


if __name__ == '__main__':
    xml_directory = './fetched_xml'
    output_directory = './json_lake'
    process_xml_files(xml_directory, output_directory)
