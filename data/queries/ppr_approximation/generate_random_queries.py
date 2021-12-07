import os
import argparse
import random

import json
from pathlib import Path
from tqdm import tqdm



def main(args):
    # Read the list of available entities to select from
    with open(args.entities_path) as f:
        data = json.load(f)

    # Extract the entities
    entities = []
    for ent in data:
        entities.append(ent)


    # Randomly select `query_size` entities
    print("There are", len(entities), 'entities to choose from')
    random.shuffle(entities)
    selected_entities = entities[:args.query_size]

    ############# Create the output query files #############

    # Create queries for single-source PPR (one entity per query)
    Path(args.output_dir + 'single_source_queries/').mkdir(parents=True, exist_ok=True)
    for i in range(len(selected_entities)):
        single_source_dict = {}
        single_source_dict['queries'] = [[selected_entities[i]]]
        with open(args.output_dir + 'single_source_queries/q_' + str(i) + '.json', 'w') as fp:
            json.dump(single_source_dict, fp, indent=4)

    # Create query file for multi-source PPR (all entities in one query)
    Path(args.output_dir + 'multi_source_query/').mkdir(parents=True, exist_ok=True)
    multi_source_dict = {}
    multi_source_dict['queries'] = [selected_entities]
    with open(args.output_dir + 'multi_source_query/query.json', 'w') as fp:
        json.dump(multi_source_dict, fp, indent=4)

        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query_size', type=int, default=5, help='Number of entities to query', required=True)
    parser.add_argument('--output_dir', help='Path to the directory where output files are saved', required=True)
    parser.add_argument('--seed', help='Random seed ', required=True)
    parser.add_argument('--entities_path', help='Path to the json file that specifies the available entities in the KB')

    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.seed:   
        print('User specified seed:', args.seed)
        random.seed(args.seed)


    main(args)