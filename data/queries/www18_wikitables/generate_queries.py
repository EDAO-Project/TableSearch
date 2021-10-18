import argparse
import json

import os
import numpy as np
import pandas as pd
import tqdm as tqdm
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

def get_query_entity_tuples(df, index_dir, data_dir):
    print("Generating query entity tuples...")

    with open(index_dir + "wikipediaLinkToEntity.json", "r") as json_file:
        wiki_links_to_ents = json.load(json_file)

    # Set of tables that were indexed
    with open(index_dir + "statistics/perTableStats.json", "r") as json_file:
        per_table_stats = json.load(json_file)
    indexed_tables = set([os.path.splitext(x)[0] for x in per_table_stats.keys()])

    # Dictionary that maps each query with the chosen tuple of entities
    query_to_tup_of_ents = {}

    # Loop over each query and find a highly relevant table from which to extract a set of represetnative entity tuples
    matching_tables = set()
    for q_id in df['query_id'].unique():
        df_tmp = df[(df['query_id'] == q_id) & (df['relevance'] > 1)]
        common_tables = set(df_tmp['table_id']) & indexed_tables
        matching_tables.update(common_tables)

        if len(common_tables) > 0:
            # For each common table find mappable entities
            table_entity_tuples = {}
            for table_id in common_tables:
                with open(data_dir + table_id+ ".json", "r") as json_file:
                    table = json.load(json_file)
                
                num_entities_per_row = per_table_stats[table_id+".json"]["numEntitiesPerRow"]
                sorted_idx = np.argsort(num_entities_per_row)[::-1]
                
                tup_of_entities = get_entity_tuple(table["rows"][sorted_idx[0]], wiki_links_to_ents)

                table_entity_tuples[table_id] = {}
                table_entity_tuples[table_id]["max_num_ents_per_row"] = max(num_entities_per_row)
                table_entity_tuples[table_id]["num_rows_with_max_num_ents_per_row"] = num_entities_per_row.count(table_entity_tuples[table_id]["max_num_ents_per_row"])
                table_entity_tuples[table_id]["tup_of_ents"] = tup_of_entities

            # Choose a tuple of entities for the current query
            # TODO: Currently we choose a tuple from a table that has the greatest number of mapped entities per row
            max_num_ents_per_row_dict = {table_id:table_entity_tuples[table_id]["max_num_ents_per_row"] for table_id in table_entity_tuples}
            if max(max_num_ents_per_row_dict.values()) > 0:
                chosen_table_id = max(max_num_ents_per_row_dict, key=max_num_ents_per_row_dict.get)
                query_to_tup_of_ents[q_id] = table_entity_tuples[chosen_table_id]
                query_to_tup_of_ents[q_id]["table_id"] = chosen_table_id 

    print("Finished generating query entity tuples\n\n")
    return query_to_tup_of_ents

def get_entity_tuple(row, wiki_links_to_ents):
    '''
    Given a row from the json get a list of its mappped entites
    '''
    tup_of_ents = []
    for cell in row:
        if len(cell["links"]) > 0:
            wikilink = cell["links"][0]
            if wikilink in wiki_links_to_ents:
                tup_of_ents.append(wiki_links_to_ents[wikilink])
    return tup_of_ents

def create_query_files(query_to_tup_of_ents, q_output_dir):
    for q_id in query_to_tup_of_ents:
        query_json = {}
        query_json['queries'] = [query_to_tup_of_ents[q_id]["tup_of_ents"]]
        
        with open("q_"+str(q_id)+".json", 'w') as fp:
            json.dump(query_json, fp, indent=4)


def main(args):

    # Extract the query relevant tables into a dataframe
    df = get_query_relevances(args.relevance_queries_path)
    df = get_statistics_of_tables(df, table_stats_dir="../../tables/wikitables/files/www18_wikitables/")
    df.to_pickle("query_relevances_df.pickle")

    # Remove tables that have less than specified number of rows and columns. Also remove queries that have 1 or fewer highly relevant tables
    df_clean = clean_query_relevances_df(df.copy(), min_rows=args.min_rows, min_cols=args.min_cols)
    df_clean.to_pickle("query_relevances_cleaned_df.pickle")

    # Extract a set of representative entity tuples for each query
    # (the entity tuples must be extract from highly relevant tables, so with a relevance score of 2) 
    query_to_tup_of_ents = get_query_entity_tuples(df=df_clean, index_dir=args.index_dir, data_dir=args.data_dir)

    create_query_files(query_to_tup_of_ents, args.q_output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-rqp', '--relevance_queries_path', help='path to the relevant datapoints', required=True)

    parser.add_argument('--min_rows', help='minimum number of rows for a table to be considered highly relevant', default=10, type=int)
    parser.add_argument('--min_cols', help='minimum number of columns for a table to be considered highly relevant', default=2, type=int)

    parser.add_argument('--index_dir', help="Path to the directory where the index step output files are saved", required=True)
    parser.add_argument('--data_dir', help="Path to the directory where the table json files are saved", required=True)
    parser.add_argument('--q_output_dir', help="Path to the directory where the generated query entity tuples are stored", required=True)

    args = parser.parse_args()

    # Create query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.q_output_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.q_output_dir):
        os.remove(os.path.join(args.q_output_dir, f))

    main(args)  