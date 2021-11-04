import os
import shutil
import argparse

import json
from pathlib import Path

def main(args):

    files = sorted(os.listdir(args.ppr_dir))
    for query_id in files:
        with open(args.ppr_dir + query_id + '/search_output/filenameToScore.json') as f:
            data = json.load(f)

        if args.top_k:
            # Extract the top-k tables
            if len(data['scores']) <= args.top_k:
                tables = [table['tableID'] for table in data['scores']]
            else:
                tables = [data['scores'][i]['tableID'] for i in range(args.top_k)]
        else:
            # Use all tables
            tables = [table['tableID'] for table in data['scores']]

        # Create the output tables for each query
        out_local_dir = args.out_dir + query_id + '/' 
        Path(out_local_dir).mkdir(parents=True, exist_ok=True)

        for table in tables:
            if Path(args.input_tables_dir + table).is_file():
                shutil.copy(args.input_tables_dir + table, out_local_dir)

    print("Finished creating top-k tables for PPR")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--ppr_dir', help='Directory where the scores for each file using PPR are stored', required=True)

    parser.add_argument('--out_dir', help='Path to the output directory where the filtered files are placed', required=True)
    parser.add_argument('--input_tables_dir', help='Path to the directory where the json files for each table are kept', required=True)
    parser.add_argument('-k', '--top_k', type=int, 
        help='Specifies the Top-k PPR tables used to be extracted into the input_tables_dir. If not specified all the tables are used')

    args = parser.parse_args()


    # Create the output directory where the tables are stored
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    shutil.rmtree(args.out_dir) 

    main(args)