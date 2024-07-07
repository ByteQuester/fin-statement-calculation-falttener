import logging
from typing import List

import pandas as pd
import networkx as nx
import re


from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess

def clean_concept_name(name):
    return re.sub(r'\(\+\d\)\s', '', name)

def rm_main(data):
    df = pd.DataFrame(data)
    # Clean concept names
    df['cleaned_concept'] = df['concept'].apply(clean_concept_name)
    df['cleaned_sub_concept'] = df['sub_concept'].apply(clean_concept_name)


    G = nx.DiGraph()

    for _, row in df.iterrows():
        G.add_edge(row['cleaned_concept'], row['cleaned_sub_concept'], weight=row['weight'])

    roots = [node for node in G.nodes() if G.in_degree(node) == 0]

    levels = {}
    for root in roots:
        for level, nodes in enumerate(nx.bfs_layers(G, root)):
            for node in nodes:
                levels[node] = level

    # Calculate the full ancestor path for each node
    def get_full_ancestor_path(node):
        ancestors = list(nx.ancestors(G, node))
        ancestors.sort(key=lambda x: levels.get(x, float('inf')))  # Sort by level
        return ancestors + [node]

    df['level'] = df['cleaned_sub_concept'].map(levels)
    df['root_concept'] = df['cleaned_sub_concept'].apply(lambda node: get_full_ancestor_path(node))

    return df  # Return the DataFrame


# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_all_tables(index_data_access: IndexDataAccess, tables: List[str] = None):
    if tables == None:
        tables = index_data_access.get_all_tables()

    for table in tables:
        logging.info(f"Processing table: {table}")
        years = index_data_access.get_all_years_for_table(table)
        for year in years:
            logging.info(f"  Processing year: {year}")
            linkroles = index_data_access.get_all_linkroles_for_year(table, year)
            for linkrole in linkroles:
                logging.info(f"    Processing linkrole: {linkrole}")
                data = index_data_access.get_data_for_linkrole(table, year, linkrole)
                if data:
                    df = rm_main(data)
                    df['year'] = year
                    df['linkrole'] = linkrole
                    index_data_access.update_table_with_results(table, df)


if __name__ == "__main__":
    index_data_access = IndexDataAccess()
    process_all_tables(index_data_access)

