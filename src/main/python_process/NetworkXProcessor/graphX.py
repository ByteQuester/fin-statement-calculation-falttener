import logging
from typing import List

import pandas as pd
import networkx as nx


from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess


def rm_main(data):
    df = pd.DataFrame(data)
    G = nx.DiGraph()

    for _, row in df.iterrows():
        G.add_edge(row['concept'], row['sub_concept'], weight=row['weight'])

    roots = [node for node in G.nodes() if G.in_degree(node) == 0]

    levels = {}
    for root in roots:
        for level, nodes in enumerate(nx.bfs_layers(G, root)):
            for node in nodes:
                levels[node] = level

    df['level'] = df['sub_concept'].map(levels)

    # Fix root_concept calculation
    df['root_concept'] = df['sub_concept'].apply(lambda node: list(nx.ancestors(G, node)) + [node])

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

