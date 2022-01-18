import shutil, os
import argparse
import pandas as pd

import json
from pathlib import Path
from tqdm import tqdm

def get_num_entities_per_table_dict(path):
    '''
    Given the path to a tableIDToEntities.ttl file return a dictionary mapping each tableID its number of entities
    '''
    print("Constructing the num_ents_per_table_dict...")

    num_ents_per_table_dict = {}

    with open(path) as file:
        for line in tqdm(file):
            vals = line.rstrip().split()
            tableID = vals[0].split('/')[-1][:-1] + '.json'
            
            if tableID not in num_ents_per_table_dict:
                num_ents_per_table_dict[tableID] = 1
            else:
                num_ents_per_table_dict[tableID] += 1

    print("Finished constructing the num_ents_per_table_dict.\n")
    return num_ents_per_table_dict

def get_filtered_wikipages_df(wikipage_tables_dict, min_num_entities_per_table, 
    min_num_tables_per_page, max_num_tables_per_page):
    '''
    Return a dataframe where each row is a wikipage and specifies the tables found in it consisted as well as the number of entities

    Parameters
    ----------
    wikipage_tables_dict (dict): Dictionary of the stats from each wikipage. This was generated by the 'extract_tables_per_wikipage.py' step
    min_num_entities_per_table (int): The minimum number of entities found in a table in order to be used in the dataset
    min_num_tables_per_page (int): The minimum number of tables found in a wikipage in order to be selected
    max_num_tables_per_page (int): The maximum number of tables found in a wikipage in order to be selected
    
    Returns
    -------
    Return a dataframe where each row is a wikipage and specifies the tables found in it that satisfy all the specified filtered conditions
    '''

    df_dict = {'wikipage': [], 'wikipage_id': [], 'num_tables': [], 'tables': [], 'num_entities': []}
    wikipage_id = 0

    # Loop over each wikipage
    for wikipage, wikipage_dict in wikipage_tables_dict.items():
    
        df_dict['wikipage'].append(wikipage)
        df_dict['wikipage_id'].append(wikipage_id)
        wikipage_id+=1
        wikipage_tables = []
        num_entities = []

        # For the current 'wikipage' identify how many tables there are with entities
        # greater or equal to 'min_num_entities_per_table' and add them to the 'tables' and 'num_entities' columns
        for tableID in wikipage_dict['tables']:
            num_ents = wikipage_dict['tables'][tableID]['num_entities']
            
            if num_ents >= min_num_entities_per_table:
                wikipage_tables.append(tableID)
                num_entities.append(num_ents)

        df_dict['tables'].append(wikipage_tables)
        df_dict['num_tables'].append(len(wikipage_tables))
        df_dict['num_entities'].append(num_entities)

    df = pd.DataFrame.from_dict(df_dict)
    print("There are", len(df.index), 'wikipages in total')

    # Remove wikipages with tables less than `min_num_tables_per_page` and more than `max_num_tables_per_page`
    df = df[(df['num_tables']>=min_num_tables_per_page) & (df['num_tables']<=max_num_tables_per_page)]
    print('After filtering there are', len(df.index), 'valid wikipages with a total of', df['num_tables'].sum(), 'table in them.')
    
    return df

def main(args):

    with open('wikipage_tables.json', 'r') as fp:
        wikipage_tables_dict = json.load(fp)

    # Construct a dataframe with all the selected wikipages and their tables and save it as a pickle file
    df = get_filtered_wikipages_df(
        wikipage_tables_dict=wikipage_tables_dict,
        min_num_entities_per_table=args.min_num_entities_per_table,
        min_num_tables_per_page=args.min_num_tables_per_page,
        max_num_tables_per_page=args.max_num_tables_per_page
    )
    df.to_pickle('wikipages_df.pickle')

    # Create a directory with the selected tables by copying them from args.wikitables_dir
    print("\nSaving the WikiPages tables at:", args.output_tables_dir, '...')
    for _, row in tqdm(df.iterrows()):
        tables = row['tables']
        for table in tables:
            shutil.copy(args.wikitables_dir+table, args.output_tables_dir+table)
    print("Finished saving the WikiPages tables\n")


 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--min_num_entities_per_table', type=int, help='The minimum number of entities found in a table in order to be used in the dataset', required=True)
    parser.add_argument('--min_num_tables_per_page', type=int, help='The minimum number of tables found in a wikipage in order to be selected', required=True)
    parser.add_argument('--max_num_tables_per_page', type=int, help='The maximum number of tables found in a wikipage in order to be selected', required=True)

    parser.add_argument('--wikitables_dir', help='Path to the where the directory containing the parsed json files of all wikitables', required=True)
    parser.add_argument('--output_tables_dir', help='Path to where the selected tables for the wikipages dataset are saved to', required=True)

    args = parser.parse_args()


    # Create the output_tables_dir if it doesn't exist (Remove all files in it if any)
    Path(args.output_tables_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.output_tables_dir):
        os.remove(os.path.join(args.output_tables_dir, f))


    main(args)