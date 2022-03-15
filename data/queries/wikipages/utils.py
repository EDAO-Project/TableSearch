import pandas as pd
import numpy as np

from tqdm import tqdm
from pathlib import Path
import json

def update_df_with_stats(df, full_df, categories_relevance_scores_dir, navigation_links_relevance_scores_dir):
    '''
    Updates the input dataframe by computing the following for each query:
    
    * categories relevant wikipages
    * categories relevant tables
    * navigation links relevant wikipages
    * navigation links relevant tables
    * categories expansion ratio
    * navigation links expansion ratio

    Parameters
    ----------
    df (pandas dataframe): Dataframe that will be updated and populated with new columns 
    full_df (pandas dataframe): Dataframe that maps each wikipages to its set of tables.
        Dataframe must have mappings to the entire search space (i.e., maps to the dataframe found in data/tables/ directory) 
    categories_relevance_scores_dir (str): Path the the relevance ground truth using wikipedia categories. If not specified ignored
    navigation_links_relevance_scores_dir (str): Path the the relevance ground truth using navigation links. If not specified ignored
    
    Returns
    -------
    Return an updated `df` dataframe
    '''

    df['categories_relevant_wikipages'] = np.nan
    df['categories_relevant_tables'] = np.nan
    df['navigation_links_relevant_wikipages'] = np.nan
    df['navigation_links_relevant_tables'] = np.nan
    df['categories_expansion_ratio'] = np.nan
    df['navigation_links_expansion_ratio'] = np.nan

    # Loop over each wikipedia page in `df` and populate the newly added columns
    for idx, row in tqdm(df.head(20).iterrows(), total=len(df.index)):
        if categories_relevance_scores_dir != None:
            # Read the categories relevance scores file
            with open(categories_relevance_scores_dir+str(row['wikipage_id'])+'.json', 'r') as fp:
                categories_relevance = json.load(fp)

                df.loc[idx, 'categories_relevant_wikipages'] = len(categories_relevance)
                categories_relevant_tables = len(get_relevant_tables(df=full_df, wikipage_id=row['wikipage_id'], relevance_scores_dir=categories_relevance_scores_dir))
                df.loc[idx, 'categories_relevant_tables'] = categories_relevant_tables

                df.loc[idx, 'categories_expansion_ratio'] = df.loc[idx, 'categories_relevant_tables'] / df.loc[idx, 'num_tables']

        if navigation_links_relevance_scores_dir != None:
            # Read the navigation links relevance scores file
            with open(navigation_links_relevance_scores_dir+str(row['wikipage_id'])+'.json', 'r') as fp:
                navigation_links_relevance = json.load(fp)
            df.loc[idx, 'navigation_links_relevant_wikipages'] = len(navigation_links_relevance)
            navigation_links_relevant_tables = len(get_relevant_tables(df=full_df, wikipage_id=row['wikipage_id'], relevance_scores_dir=navigation_links_relevance_scores_dir))
            df.loc[idx, 'navigation_links_relevant_tables'] = navigation_links_relevant_tables

            df.loc[idx, 'navigation_links_expansion_ratio'] = df.loc[idx, 'navigation_links_relevant_tables'] / df.loc[idx, 'num_tables'] 

    return df

def get_relevant_tables(df, wikipage_id, relevance_scores_dir):
    '''
    Given a query from a `wikipage` return a list of all its relevant tables using the specified `relevance_scores_dir`
    '''
    with open(relevance_scores_dir+str(wikipage_id)+'.json', 'r') as fp:
        relevance_dict = json.load(fp)
    
    relevant_tables = []
    for wikipage_name in relevance_dict:
        tables_row = df.loc[df['wikipage']=='https://en.wikipedia.org/wiki/' + wikipage_name]['tables'].values
        if len(tables_row) > 0:
            relevant_tables += tables_row[0]
        else:
            # No known tables for this wikipage, continue to the next wikipage
            continue
    return relevant_tables


def get_filename_to_entities_dict(path):
    '''
    Given the path to a tableIDToEntities.ttl file return a dictionary mapping each tableID to its entities
    '''

    filename_to_entities_dict = {}

    with open(path) as file:
        for line in tqdm(file):
            vals = line.rstrip().split()
            tableID = vals[0].split('/')[-1][:-1] + '.json'
            entity = vals[2][1:-1]
            
            if tableID not in filename_to_entities_dict:
                filename_to_entities_dict[tableID] = set()
                filename_to_entities_dict[tableID].add(entity)
            else:
                filename_to_entities_dict[tableID].add(entity)

    return filename_to_entities_dict

def get_query_entities(path):
    '''
    Return as a set the entities found in the query file
    '''

    with open(path, 'r') as f:
        data = json.load(f)['queries']

    entities = set()
    for tuple in data:
        for ent in tuple:
            entities.add(ent)

    return entities

def get_query_containment(df, filename_to_entities_dict, queries_dir):
    '''
    Updates the `df` dataframe with a new column `avg_query_containment`
    '''

    for idx, row in tqdm(df.iterrows(), total=len(df.index)):
        selected_table = row['selected_table']

        query_path = queries_dir + 'wikipage_' + str(row['wikipage_id'])+'.json'
        if Path(query_path).is_file():
            query_entities = get_query_entities(query_path)

            relevant_tables = row['tables'].copy()
            relevant_tables.remove(selected_table)

            containment_scores = []
            for table in relevant_tables:
                table_ents = filename_to_entities_dict[table]

                containment = len(table_ents & query_entities) / len(query_entities)
                containment_scores.append(containment)

            df.loc[idx, 'avg_query_containment'] = np.array(containment_scores).mean()

    return df