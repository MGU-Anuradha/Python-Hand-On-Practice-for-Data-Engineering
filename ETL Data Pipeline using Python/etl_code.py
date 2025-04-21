import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Log file and output file
log_file = "log_file.txt"
target_file = "transformed_data.csv"

########### Extraction Functions ###############

# Extract data from a CSV file
def extract_from_csv(file_path):
    return pd.read_csv(file_path)

# Extract data from a JSON file
def extract_from_json(file_path):
    return pd.read_json(file_path, lines=True)

# Extract data from an XML file
def extract_from_xml(file_path):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_path)
    root = tree.getroot()

    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        row = pd.DataFrame([{"name": name, "height": height, "weight": weight}])
        dataframe = pd.concat([dataframe, row], ignore_index=True)

    return dataframe

# Main extract function that handles all file types
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # Process all CSV files (skip the target output file)
    for csv_file in glob.glob("*.csv"):
        if csv_file != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csv_file)], ignore_index=True)

    # Process all JSON files
    for json_file in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(json_file)], ignore_index=True)

    # Process all XML files
    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xml_file)], ignore_index=True)

    return extracted_data

########### Transformation Function ###############

def transform(data):
    # Convert inches to meters (1 inch = 0.0254 meters) and round to 2 decimals
    data['height'] = round(data['height'] * 0.0254, 2)

    # Convert pounds to kilograms (1 pound = 0.45359237 kg) and round to 2 decimals
    data['weight'] = round(data['weight'] * 0.45359237, 2)

    return data

########### Load Function ###############

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

########### Logging Function ###############

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-MonthName-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp},{message}\n")

########### ETL Pipeline Execution ###############

# Log the start of the ETL job
log_progress("ETL Job Started")

# Extraction
log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

# Transformation
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)
log_progress("Transform phase Ended")

# Loading
log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

# Completion
log_progress("ETL Job Ended")
