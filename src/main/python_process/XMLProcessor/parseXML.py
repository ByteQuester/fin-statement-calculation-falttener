import os
import subprocess
import json

def get_xml_files(directory):
    return [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith('.xml')]

def run_arelle_command(xml_file, output_json_file, arelle_path='/Users/stewie/Downloads/Arelle-master/arelleCmdLine.py', python_path='/usr/bin/python3'):
    command = [
        python_path, arelle_path,
        '--file', xml_file,
        '--cal', output_json_file
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Successfully processed {xml_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {xml_file}: {e.stderr}")

def process_xml_file(args):
    xml_file, output_directory, arelle_path, python_path = args
    output_json_file = os.path.join(output_directory, os.path.basename(xml_file).replace('.xml', '.json'))
    run_arelle_command(xml_file, output_json_file, arelle_path, python_path)
    with open(output_json_file, 'r') as f:
        json.dump(output_json_file, f, indent=2)
