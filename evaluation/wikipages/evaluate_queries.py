import pandas as pd
import numpy as np
import json
import os
import random
import argparse
import itertools

from pathlib import Path

from sklearn.metrics import ndcg_score
from tqdm import tqdm

import utils


def get_n_random_tables(n, exclude_list, tables_list, seed=0):
    '''
    Randomly select n different tables from the tables_list. The tables selected must not be in the exclude_list
    '''
    random.seed(seed)
    selected_tables = []

    while len(selected_tables) < n:
        idx = random.randrange(len(tables_list))

        if ( (tables_list[idx] not in selected_tables) and (tables_list[idx] not in exclude_list)):
            selected_tables.append(tables_list[idx])
    
    return selected_tables

def get_ndcg_scores_over_labeled(df, scores_path, tables_path, seed=0):
    '''
    Compute the NDCG scores for every query scored in the `scores_path`

    The NDCG score is computed on the following groundtruth:
        * Tables from the same wikipedia page as the query are marked as relevant (relevance score 1)
        * k randomly chosen tables not from the current wikipedia page as marked as not-relevant (relevance score 0).
          k is equal to the number of tables in the current wikipedia page


    Return a dictionary with the ndcg score for each wikipage_id (i.e., for each query)
    '''

    # List of all tables in the WikiPages dataset
    tables_list = [table for table in os.listdir(tables_path)]

    # Maps a wikipage_id to its respective ndcg score
    ndcg_scores_dict = {}

    # Loop over each query output and compute a top-k NDCG score where k is 2 times the number of truly relevant tables
    for file in tqdm(os.listdir(scores_path)):
        filepath = Path(scores_path+file+'/search_output/filenameToScore.json')
        if filepath.is_file():
            wikipage_id = int(file.split('_')[1])
            
            # Set k to evaluate the query (k is set to k_multiplier times the truly relevant tables)
            num_tables = len(df[df['wikipage_id']==wikipage_id]['tables'].values[0])
            
            # Read scores
            with open(scores_path + file + '/search_output/filenameToScore.json', 'r') as fp:
                scored_tables_json = json.load(fp)['scores']
            
            table_id_to_score = {}
            for dic in scored_tables_json:
                table_id_to_score[dic['tableID']]=dic['score']

            # Construct groundtruth relevance scores
            gt_relevance = np.array([[1]*num_tables + [0]*num_tables])
            
            # Randomly select `num_tables` other tables as non-relevant
            gt_exclude_list = df[df['wikipage_id']==wikipage_id]['tables'].values[0]
            random_non_relevant_tables = get_n_random_tables(
                n = num_tables,
                exclude_list = gt_exclude_list,
                tables_list = tables_list,
                seed = seed
            )
            gt_relevance_tables = gt_exclude_list + random_non_relevant_tables

            predicted_relevance = np.array([[table_id_to_score[table_id] for table_id in gt_relevance_tables]])

            score = ndcg_score(gt_relevance, predicted_relevance)
            ndcg_scores_dict[wikipage_id] = score
        else:
            # Ignore this query if scores not found
            pass

    return ndcg_scores_dict



def get_ndcg_scores_over_output(full_df, query_df, scores_path, groundtruth_relevance_scores_dir, tables_list,
    remove_query_tables_from_evaluation_mode=None, k=10):
    '''
    Compute the NDCG scores for every query scored in the `scores_path`

    The NDCG score is computed over the top-k results from the output of each query

    If `remove_query_tables_from_evaluation_mode` is specified then query tables are not evaluated and removed from the groundtruth

    Return a dictionary indexed by wikipage_id that maps to a dictionary specifying the
    ndcg_score@k, as well as the number of true relevant tables found at k
    '''

    # Maps a wikipage_id to its respective ndcg score
    scores_dict = {}

    # Loop over each query output and compute a top-k NDCG score
    for file in tqdm(sorted(os.listdir(scores_path))):
        filepath = Path(scores_path+file+'/search_output/filenameToScore.json')
        if filepath.is_file():
            wikipage_id = int(file.split('_')[1])

            filtered_tables_list = tables_list
            # Update the `filtered_tables_list` if `remove_query_tables_from_evaluation_mode` is specified
            if args.remove_query_tables_from_evaluation_mode:
                filtered_tables_list, tables_removed = utils.filter_tables_list(
                    query_df=query_df, tables_list=tables_list,
                    mode=args.remove_query_tables_from_evaluation_mode, wikipage_id=wikipage_id
                )

            # Get the gt_to_relevance_scores_dict
            gt_tables_to_relevance_scores_dict = utils.evaluation_helpers.get_gt_tables_to_relevance_scores_dict(
               full_df=full_df, wikipage_id=wikipage_id, 
               groundtruth_relevance_scores_dir=groundtruth_relevance_scores_dir, tables_list=filtered_tables_list
            )

            # Read scores and populate table_id_to_pred_score
            with open(scores_path + file + '/search_output/filenameToScore.json', 'r') as fp:
                scored_tables_json = json.load(fp)['scores']
            table_id_to_pred_score = {table_dict['tableID']:table_dict['score'] for table_dict in scored_tables_json}

            # Ensure there are k or more scored tables in the 'scored_tables_json'
            assert len(scored_tables_json)>=k, 'There are less than k tables that have been scored for wikipage_id: ' + str(wikipage_id)


            # Construct the predicted relevance scores by querying the predicted score
            # for all tables specified in the `gt_to_relevance_scores_dict`
            pred_tables_to_relevance_scores_dict = {table:0 for table in gt_tables_to_relevance_scores_dict}
            for table in gt_tables_to_relevance_scores_dict:
                pred_tables_to_relevance_scores_dict[table] = table_id_to_pred_score[table]

            # Compute the num_relevant tables in the top-k of the `table_id_to_pred_score` dictionary
            num_relevant_tables = 0
            for table in list(table_id_to_pred_score.keys())[:k]:
                if gt_tables_to_relevance_scores_dict[table] > 0:
                    num_relevant_tables+=1

            
            gt_relevance = np.array([list(gt_tables_to_relevance_scores_dict.values())])
            predicted_relevance = np.array([list(pred_tables_to_relevance_scores_dict.values())])

            score = ndcg_score(gt_relevance, predicted_relevance, k=k)
            scores_dict[wikipage_id] = {'ndcg': score, 'num_relevant_tables': num_relevant_tables}
        else:
            # Ignore this query if scores not found
            pass
        
    return scores_dict

# def filter_tables_list(query_df, tables_list, mode, wikipage_id):
#     '''
#     Returns an updated `tables_list` based on the specified `mode` of removing query tables from the evaluation
#     as well as a list of the tables removed
#     '''
#     row = query_df[query_df['wikipage_id']==wikipage_id]
#     tables_to_remove = []
#     if mode == 'remove_query_table':
#         tables_to_remove.extend(row['selected_table'])
#     elif mode == 'remove_query_wikipage_tables':
#         [tables_to_remove.append(table) for table in row['tables'].to_list()[0]]

#     # Remove all `tables_to_remove` from `tables_list`
#     tables_list = [table for table in tables_list if table not in tables_to_remove]

#     return tables_list, tables_to_remove

def main(args):

    df = pd.read_pickle(args.query_df)
    full_df = pd.read_pickle(args.full_df)  

    # Extract the names of all tables in our search space
    tables_list = os.listdir(args.tables_dir)

    scores_over_output = get_ndcg_scores_over_output(
        full_df=full_df,
        query_df=df,
        scores_path=args.scores_dir,
        groundtruth_relevance_scores_dir=args.groundtruth_relevance_scores_dir,
        tables_list=tables_list,
        remove_query_tables_from_evaluation_mode=args.remove_query_tables_from_evaluation_mode,
        k=args.topk
    )

    with open(args.output_dir + 'scores_over_output_' + str(args.topk) + '.json', 'w') as fp:
        json.dump(scores_over_output, fp, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--output_dir', help='Path to the directory where the scores .json file is saved', required=True)
    parser.add_argument('--query_df', help='Path to the queries dataframe (i.e., the list of wikipages that are evaluated)', required=True)
    parser.add_argument('--scores_dir', help='Path to the directory where the scores for each table are stored', required=True)
    parser.add_argument('--full_df', help='Path to the full queries dataframe (i.e., it provides mappings for each wikipage to its set of tables', required=True)
    parser.add_argument('--groundtruth_relevance_scores_dir', help='Path to the directory that contains the groundtruth relevance scores for each wikipage', required=True)
    parser.add_argument('--tables_dir', help='Path to the directory containing all the tables that make up the search space for all queries', required=True)
    parser.add_argument('--topk', type=int, default=10, help='Specifies the top-k value for which NDCG scores are evaluated')
    
    parser.add_argument('--remove_query_tables_from_evaluation_mode', choices=['remove_query_table', 'remove_query_wikipage_tables'], 
        help='If specified then the query wikitable or all tables found in the query wikipage (depending on which mode was specified) \
        are removed from the evaluation')
    args = parser.parse_args()

    # Create the query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    print("\nOutput Directory:", args.output_dir)
    print("Query Dataframe:", args.query_df)
    print("Scores Directory:", args.scores_dir)
    print("Full Dataframe:", args.full_df)
    print("Tables Directory:", args.tables_dir)
    print("Top-k:", args.topk)
    if args.remove_query_tables_from_evaluation_mode:
        print('Remove Query Tables from Evaluation Mode:', args.remove_query_tables_from_evaluation_mode)
    print('\n')

    main(args)  