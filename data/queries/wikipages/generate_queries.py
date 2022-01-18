import argparse
import json

import os
import shutil
import numpy as np
import pandas as pd

from tqdm import tqdm
from pathlib import Path

def create_query_files(query_to_list_of_tuples, q_output_dir):
    for q_id in query_to_list_of_tuples:
        query_json = {}
        query_json['queries'] = query_to_list_of_tuples[q_id]
        
        with open(q_output_dir + "wikipage_"+str(q_id)+".json", 'w') as fp:
            json.dump(query_json, fp, indent=4)

def get_entity_tuple(row, wiki_links_to_ents):
    '''
    Given a row from the json get a list of its mapped entities
    '''
    tup_of_ents = []
    for cell in row:
        if len(cell["links"]) > 0:
            wikilink = cell["links"][0]
            if wikilink in wiki_links_to_ents:
                tup_of_ents.append(wiki_links_to_ents[wikilink])
    return tup_of_ents


def get_rows_with_max_num_unique_ents(table, wiki_links_to_ents, known_entity_embeddings=None):
    '''
    Given a json of `table` and the dictionary of the `wiki_links_to_ents` find the set of rows/tuples in `table`
    for which the number of unique entities in a row is maximal.

    If `known_entity_embeddings` is not None then only consider tuples for which all entities are found in `known_entity_embeddings`

    Returns 3 items:

    'max_num_unique_ents_per_row' (int): The maximum number of unique entities mapped in a any row in the current table
    'num_rows_with_max_num_unique_ents' (int): The number of rows that have the maximum number of unique entities
    'row_ids' (list of int): A list with the row IDs that have the maximal number of unique entities per row
    '''
    df_dict = {'row_id': [], 'num_ents': []}
    for i in range(len(table["rows"])):
        ents = set(get_entity_tuple(table["rows"][i], wiki_links_to_ents))
        num_ents = len(ents)
        if known_entity_embeddings:
            # Ensure that all entities in `ents` are in known_entity_embeddings otherwise set `num_ents` to zero
            for ent in ents:
                if ent not in known_entity_embeddings:
                    num_ents = 0

        df_dict['row_id'].append(i)
        df_dict['num_ents'].append(num_ents)

    # Extract the desired outputs from the dataframe
    df = pd.DataFrame.from_dict(df_dict)
    max_num_unique_ents_per_row = df['num_ents'].max()
    df = df[df['num_ents'] == max_num_unique_ents_per_row]
    row_ids = df['row_id'].tolist()

    return max_num_unique_ents_per_row, len(df.index), row_ids

def get_query_entity_tuples(df, tables_dir, wiki_links_to_ents, tuples_per_query):
    '''
    Given a dataframe of the queries and their relevant tables extract a set of represetnative tuples as queries
    '''

    # Dictionary that maps each query with a list of the chosen tuples of entities
    query_to_list_of_tuples = {}

    print("\nGenerating a query from each wikipage...")
    for _, df_row in tqdm(df.iterrows(), total=len(df.index)):

        relevant_tables = df_row['tables']

        table_to_max_ents_dict = {'table_id': [], 'max_num_unique_ents_per_row': [], 'num_rows_with_max_num_unique_ents': [], 'row_ids': []}
        for table_id in relevant_tables:
            with open(tables_dir + table_id, "r") as json_file:
                table = json.load(json_file)

            # Find the rows with the maximal number of unique entities for the current table
            max_num_unique_ents_per_row, num_rows_with_max_num_unique_ents, row_ids = get_rows_with_max_num_unique_ents(table, wiki_links_to_ents)
            table_to_max_ents_dict["table_id"].append(table_id)
            table_to_max_ents_dict["max_num_unique_ents_per_row"].append(max_num_unique_ents_per_row)
            table_to_max_ents_dict["num_rows_with_max_num_unique_ents"].append(num_rows_with_max_num_unique_ents)
            table_to_max_ents_dict["row_ids"].append(row_ids)

        # Choose the table that has rows with the highest number of unique entities
        table_to_max_ents_df = pd.DataFrame.from_dict(table_to_max_ents_dict)
        table_to_max_ents_df = table_to_max_ents_df.sort_values(['max_num_unique_ents_per_row', 'num_rows_with_max_num_unique_ents'], ascending=[False, False])
        best_table_row = table_to_max_ents_df.head(1)


        # Ensure that the 'max_num_unique_ents_per_row' is greater than 0
        if max(best_table_row['max_num_unique_ents_per_row']) > 0:
            with open(tables_dir + best_table_row['table_id'].values[0], "r") as json_file:
                best_table = json.load(json_file)
            
            if tuples_per_query == 'single':
                # Only the first row in the best_table['row_ids'] is chosen as the tuple
                selected_rows = [best_table_row['row_ids'].values[0][0]]
            elif tuples_per_query == 'all':
                # All rows in the best_table['row_ids'] are chosen as the tuples
                selected_rows = best_table_row['row_ids'].values[0]

            selected_tuples = [get_entity_tuple(best_table['rows'][row_id], wiki_links_to_ents) for row_id in selected_rows]
            query_to_list_of_tuples[df_row['wikipage_id']] = selected_tuples
    print("Finished generating queries\n")

    return query_to_list_of_tuples


def main(args):
    df = pd.read_pickle(args.wikipages_df)

    with open(args.wikilink_to_entity, "r") as json_file:
        wiki_links_to_ents = json.load(json_file)

    # Dictionary that maps each query with a list of the chosen tuples of entities
    query_to_list_of_tuples = get_query_entity_tuples(
        df = df,
        tables_dir=args.tables_dir,
        wiki_links_to_ents=wiki_links_to_ents,
        tuples_per_query=args.tuples_per_query
    )

    # Create a query json file for each query
    create_query_files(query_to_list_of_tuples, args.q_output_dir)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file summarizing the wikipages to be used', required=True)
    parser.add_argument('--tables_dir', help='Path to the parsed table json files', required=True)
    parser.add_argument('--q_output_dir', help="Path to the directory where the generated query entity tuples are stored", required=True)
    parser.add_argument('--wikilink_to_entity', help="Path to the wikipediaLinkToEntity.json file", required=True)

    parser.add_argument('--tuples_per_query', choices=['single', 'all'], default='single',
        help="Specifies if only one or all of the best found tuples are used as the query tuples",
    )

    args = parser.parse_args()

    # Create the query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.q_output_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.q_output_dir):
        os.remove(os.path.join(args.q_output_dir, f))

    main(args)  