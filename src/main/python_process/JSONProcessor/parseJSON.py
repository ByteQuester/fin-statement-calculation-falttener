import os
import json
import pandas as pd
import re

from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess


def process_calculations_with_hierarchy(directory):
    """
    Processes calculations in a directory, extracting concept hierarchies and creating DataFrames for each CIK.

    Args:
        directory: The directory containing the calculation JSON files.

    Returns:
        dict: A dictionary where keys are CIK numbers and values are the corresponding DataFrames.
    """
    cik_dataframes = {}

    def process_concepts(concept_list, parent_label=None, concept_hierarchy=None):
        """Processes concepts recursively to build the hierarchy."""
        if concept_hierarchy is None:
            concept_hierarchy = {}  # Initialize if not provided

        for item in concept_list:
            if isinstance(item, list) and item[0] == "concept":
                name = item[1]["name"]
                label = item[1].get("label", name)
                concept_hierarchy[label] = []
                if parent_label:
                    concept_hierarchy[parent_label].append(label)
                if len(item) > 2:
                    process_concepts(item[2:], label, concept_hierarchy)

        return concept_hierarchy  # Return the updated dictionary

    # --- Main Processing Loop ---
    for filename in os.listdir(directory):
        if filename.startswith("cal_") and filename.endswith(".json"):
            cik_number, year = filename.split("_")[1:3]
            year = year.split(".")[0]

            with open(os.path.join(directory, filename), "r") as file:
                data = json.load(file)

            # Extract concept hierarchy using the given function
            df = extract_concept_hierarchy_with_weights(data, process_concepts)  # Pass process_concepts
            df['cik'] = cik_number  # Add cik to the dataframe
            df['year'] = year  # Add year to the dataframe

            # Store the DataFrame
            if cik_number not in cik_dataframes:
                cik_dataframes[cik_number] = df
            else:
                cik_dataframes[cik_number] = pd.concat([cik_dataframes[cik_number], df])

    return cik_dataframes


def extract_concept_hierarchy_with_weights(data, process_concepts, link_role_index=None):
    """
    Extracts concept hierarchy with weights from a calculation linkbase in XBRL data.

    Args:
        data (dict): The loaded JSON data containing the XML Cal.
        process_concepts (function): Function to process concepts and build hierarchy.
        link_role_index (int, optional): Index of the specific link role to process.
                                        If None, processes all link roles.

    Returns:
        pandas.DataFrame: A DataFrame containing concepts, sub-concepts, weights, and link roles.
    """
    all_rows = []

    if link_role_index is not None:
        link_roles = [data["calculationLinkbase"][link_role_index]]
    else:
        link_roles = data["calculationLinkbase"]

    for link_role in link_roles:
        concepts = link_role[2:]
        link_role_name = link_role[1]["role"].split("/")[-1]

        concept_hierarchy = process_concepts(concepts)  # Use the provided function

        for label, sub_concepts in concept_hierarchy.items():
            for sub_concept in sub_concepts:
                weight = None
                match = re.match(r"^(\([\+\-]?\d+\)) (.+)$", sub_concept)
                if match:
                    weight = match.group(1)[1:-1]  # Extract the weight value from the string
                    sub_concept = match.group(2)  # Get the actual sub-concept name

                all_rows.append([link_role_name, label, sub_concept, weight])

    df = pd.DataFrame(all_rows, columns=["linkrole", "concept", "sub_concept", "weight"])
    return df


if __name__ == '__main__':
    directory_path = "../XMLProcessor/json_lake"
    cik_dataframes = process_calculations_with_hierarchy(directory_path)
    IndexDataAccess().create_and_populate_mysql_database(cik_dataframes)
