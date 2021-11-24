import os
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

def main(args):

    wikipage_tables_dict = {}

    # Get a dictionary of the number of entities per table
    num_ents_per_table_dict = get_num_entities_per_table_dict(args.table_id_to_entities_path)

    files = sorted(os.listdir(args.input_tables_dir))
    for file in tqdm(files):
        with open(args.input_tables_dir + file) as f:
            data = json.load(f)
        
        wikipage = "https://en.wikipedia.org/wiki/"+data['pgTitle'].replace(' ', '_')

        # Get the number of entities found in the table `file`
        if file in num_ents_per_table_dict:
            num_ents = num_ents_per_table_dict[file]
        else:
            num_ents = 0
        
        # Update the `wikipage_tables_dict` dictionary
        if wikipage not in wikipage_tables_dict:
            wikipage_tables_dict[wikipage] = {
                'tables': { file: {'num_entities': num_ents} }
            }
        else:
            wikipage_tables_dict[wikipage]['tables'][file] = {'num_entities': num_ents}

    for wikipage in wikipage_tables_dict:
        num_tables = len(wikipage_tables_dict[wikipage]['tables'])
        wikipage_tables_dict[wikipage]['num_tables'] = num_tables

    # Save the `wikipage_tables_dict` as a json file
    print("Saving wikipage_tables dictionary to wikipage_tables.json")
    with open('wikipage_tables.json', 'w') as fp:
        json.dump(wikipage_tables_dict, fp, indent=4)
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_tables_dir', help='Path to the directory where the json files for each table are kept', required=True)
    parser.add_argument('--table_id_to_entities_path', help='Path to the tableIDToEntities.ttl file corresponding to \
        the tables specified in the `input_tables_dir`', required=True)

    args = parser.parse_args()


    main(args)