import argparse
import json

import os
import shutil
import numpy as np
import pandas as pd

from tqdm import tqdm
from pathlib import Path



def get_query_relevances(query_path):
    header_list = ["query_id", "column2", "table_id", "relevance"]
    df = pd.read_csv(query_path, sep='\t', header=None, names=header_list)
    return df

def get_statistics_of_tables(df, table_stats_dir):
    '''
    Get the statistics of the query relevant tables and update the dataframe with the respective
    number of columns, rows and number of numeric columns per table 
    '''
    
    num_columns = []
    num_lines = []
    num_numeric_columns = []
    df['num_columns'] = np.nan
    df['num_rows'] = np.nan
    df['num_numeric_columns'] = np.nan

    for table_id in df['table_id']:
        if table_id[0] == "t":
            with open(table_stats_dir + table_id + '.json') as f:
                table = json.load(f)
        
            num_columns.append(table['numCols'])
            num_lines.append(table['numDataRows'])
            num_numeric_columns.append(len(table['numericColumns']))
        else:
            num_columns.append(-1)
            num_lines.append(-1)
            num_numeric_columns.append(-1)
    
    df['num_columns'] = num_columns
    df['num_rows'] = num_lines
    df['num_numeric_columns'] = num_numeric_columns

    # Print summary statistics over the dataframe
    print("STATISTICS OF TABLES WITH KNOWN GROUND TRUTH")
    print("[Number of Columns] MAX: {} ".format(df['num_columns'].max()), 
        "MIN: {:.3f} ".format(df['num_columns'].min()),
        "AVG: {:.3f} ".format(df['num_columns'].mean()),
        "STD: {:.3f} ".format(df['num_columns'].std()),
        "MED: {:.3f} ".format(df['num_columns'].median()))

    print("[Number of Rows] MAX: {} ".format(df['num_rows'].max()), 
        "MIN: {:.3f} ".format(df['num_rows'].min()),
        "AVG: {:.3f} ".format(df['num_rows'].mean()),
        "STD: {:.3f} ".format(df['num_rows'].std()),
        "MED: {:.3f} ".format(df['num_rows'].median()))

    print("[Number of Numeric Columns] MAX: {} ".format(df['num_numeric_columns'].max()), 
        "MIN: {:.3f} ".format(df['num_numeric_columns'].min()),
        "AVG: {:.3f} ".format(df['num_numeric_columns'].mean()),
        "STD: {:.3f} ".format(df['num_numeric_columns'].std()),
        "MED: {:.3f} ".format(df['num_numeric_columns'].median()), '\n\n')

    return df


def clean_query_relevances_df(df, min_cols=1, min_rows=1):
    '''
    Remove query tables that have less than 'min_cols' and/or less than 'min_rows'

    Also remove any queries that have 1 or fewer highly relevant tables
    '''

    print("Cleaning query relevances dataframe...")

    original_df_size = len(df.index)

    # Remove tables that have less than specified number of rows and columns
    df = df[df['num_columns'] >= min_cols]
    df = df[df['num_rows'] >= min_rows]

    # Loop over each query and remove them if they have 1 or fewer highly relevant tables
    new_df = df.copy()
    for q_id in df['query_id'].unique():
        df_tmp = df[(df['query_id'] == q_id) & (df['relevance']>1)]
        if len(df_tmp.index) <= 1:
            # There is only 1 or zero highly relevant tables for query 'q_id', remove the query and all its associated tables
            new_df = new_df.drop(new_df[new_df['query_id'] == q_id].index)


    new_df_size = len(new_df.index)
    print("Removed", original_df_size - new_df_size, "rows from the query relevances df")
    print("There are now", new_df['query_id'].nunique(), 'queries in total with', new_df['table_id'].nunique(), 'unique tables')
    print("Finished cleaning query relevances dataframe\n\n")

    return new_df

def get_query_entity_tuples(df, index_dir, data_dir, tuples_per_query, known_entity_embeddings=None):
    '''
    Given a dataframe of the queries and their relevant tables extract a set of represetnative tuples as queries

    If known_entity_embeddings is not None then the tuples extracted must all have entities found in `known_entity_embeddings`
    '''

    print("Generating query entity tuples...")
    with open(index_dir + "wikipediaLinkToEntity.json", "r") as json_file:
        wiki_links_to_ents = json.load(json_file)

    # Set of tables that were indexed
    with open(index_dir + "statistics/perTableStats.json", "r") as json_file:
        per_table_stats = json.load(json_file)
    indexed_tables = set([os.path.splitext(x)[0] for x in per_table_stats.keys()])

    # Dictionary that maps each query with a list of the chosen tuples of entities
    query_to_list_of_tuples = {}

    # Loop over each query and find a highly relevant table from which to extract a set of represetnative entity tuples
    matching_tables = set()
    for q_id in tqdm(df['query_id'].unique()):
        df_tmp = df[(df['query_id'] == q_id) & (df['relevance'] > 1)]
        common_tables = set(df_tmp['table_id']) & indexed_tables
        matching_tables.update(common_tables)

        if len(common_tables) > 0:
            # For each common table find mappable entities
            table_to_max_ents_dict = {'table_id': [], 'max_num_unique_ents_per_row': [], 'num_rows_with_max_num_unique_ents': [], 'row_ids': []}
            for table_id in common_tables:
                with open(data_dir + table_id+ ".json", "r") as json_file:
                    table = json.load(json_file)

                # Find the rows with the maximal number of unique for the current table
                max_num_unique_ents_per_row, num_rows_with_max_num_unique_ents, row_ids = get_rows_with_max_num_unique_ents(table, wiki_links_to_ents, known_entity_embeddings)
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
                with open(data_dir + best_table_row['table_id'].values[0] + ".json", "r") as json_file:
                    best_table = json.load(json_file)
                
                if tuples_per_query == 'single':
                    # Only the first row in the best_table['row_ids'] is chosen as the tuple
                    selected_rows = [best_table_row['row_ids'].values[0][0]]
                elif tuples_per_query == 'all':
                    # All rows in the best_table['row_ids'] are chosen as the tuples
                    selected_rows = best_table_row['row_ids'].values[0]

                selected_tuples = [get_entity_tuple(best_table['rows'][row_id], wiki_links_to_ents) for row_id in selected_rows]
                query_to_list_of_tuples[q_id] = selected_tuples

    print("Finished generating query entity tuples\n\n")
    return query_to_list_of_tuples

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

def create_query_files(query_to_list_of_tuples, q_output_dir):
    for q_id in query_to_list_of_tuples:
        query_json = {}
        query_json['queries'] = query_to_list_of_tuples[q_id]
        
        with open(q_output_dir + "q_"+str(q_id)+".json", 'w') as fp:
            json.dump(query_json, fp, indent=4)


def create_filtered_tables(query_ids, df, data_dir, filtered_tables_output_dir):
    '''
    Creates a subdirectory for each query under the `filtered_tables_output_dir` directory with all
    the tables referenced for a query in the groundtruth dataframe `df`
    '''
    for q_id in query_ids:
        out_dir = args.filtered_tables_output_dir + 'q_' + str(q_id) + '/' 
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        tables_to_copy = df[df['query_id'] == q_id]['table_id'].values
        for table in tables_to_copy:
            if Path(data_dir + table + '.json').is_file():
                shutil.copy(data_dir + table + '.json', out_dir)
                

def main(args):
    # If the `embeddings_path` is specified then ensure all selected query tuples map to entities in `known_entity_embeddings`
    known_entity_embeddings = None
    if args.embeddings_path:
        print("Loading the known embeddings file...")
        with open(args.embeddings_path) as json_file:
            known_entity_embeddings = json.load(json_file)
            known_entity_embeddings = set(known_entity_embeddings.keys())
        print("Finished loading the known embeddings file\n")


    # Extract the query relevant tables into a dataframe
    df = get_query_relevances(args.relevance_queries_path)
    df = get_statistics_of_tables(df, table_stats_dir='../../tables/wikitables/files/wikitables/')
    df.to_pickle("query_relevances_df.pickle")

    # Remove tables that have less than specified number of rows and columns. Also remove queries that have 1 or fewer highly relevant tables
    df_clean = clean_query_relevances_df(df.copy(), min_rows=args.min_rows, min_cols=args.min_cols)
    df_clean.to_pickle("query_relevances_cleaned_df.pickle")

    # Extract a set of representative entity tuples for each query
    # (the entity tuples must be extracted from highly relevant tables, so with a relevance score of 2) 
    query_to_list_of_tuples = get_query_entity_tuples(
        df=df_clean,
        index_dir=args.index_dir,
        data_dir=args.data_dir,
        tuples_per_query=args.tuples_per_query,
        known_entity_embeddings=known_entity_embeddings)

    # Create a query json file for each query
    create_query_files(query_to_list_of_tuples, args.q_output_dir)

    # Create the set of filtered tables for each query
    create_filtered_tables(
        query_ids=query_to_list_of_tuples.keys(),
        df=df_clean,
        data_dir=args.data_dir,
        filtered_tables_output_dir=args.filtered_tables_output_dir
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-rqp', '--relevance_queries_path', help='path to the relevant datapoints', required=True)

    parser.add_argument('--min_rows', help='minimum number of rows for a table to be considered highly relevant', default=10, type=int)
    parser.add_argument('--min_cols', help='minimum number of columns for a table to be considered highly relevant', default=2, type=int)

    parser.add_argument('--index_dir', help="Path to the directory where the indexing step output files are located", required=True)
    parser.add_argument('--data_dir', help="Path to the directory where the parsed table json files are saved", required=True)
    parser.add_argument('--q_output_dir', help="Path to the directory where the generated query entity tuples are stored", required=True)
    parser.add_argument('--filtered_tables_output_dir', help="Path to the directory where the filtered table datasets for each query are stored", required=True)
    parser.add_argument('--embeddings_path', help="Path to the available pre-trained embeddings of entities. \
        If specified all entities in the selected query tuples must be mappable to a pre-trained embedding")

    parser.add_argument('--tuples_per_query', choices=['single', 'all'], default='single',
        help="Specifies if only one or all of the best found tuples are used as the query tuples",
    )

    args = parser.parse_args()

    # Create the query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.q_output_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.q_output_dir):
        os.remove(os.path.join(args.q_output_dir, f))

    # Create the directory where the filtered tables for each query are stored (Remove all data in it if any)
    Path(args.filtered_tables_output_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(args.filtered_tables_output_dir) 

    main(args)  