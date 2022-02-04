import pandas as pd
import numpy as np
import json
import os
import random
import argparse

from pathlib import Path

from sklearn.metrics import ndcg_score
from tqdm import tqdm


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



def get_ndcg_scores_over_output(df, scores_path, k=10):
    '''
    Compute the NDCG scores for every query scored in the `scores_path`

    The NDCG score is computed over the top-k results from the output of each query

    Return a dictionary indexed by wikipage_id that maps to a dictionary specifying the
    ndcg_score@k, as well as the number of true relevant tables found at k
    '''

    # Maps a wikipage_id to its respective ndcg score
    scores_dict = {}

    # Loop over each query output and compute a top-k NDCG score where k is 2 times the number of truly relevant tables
    for file in tqdm(os.listdir(scores_path)):
        filepath = Path(scores_path+file+'/search_output/filenameToScore.json')
        if filepath.is_file():
            wikipage_id = int(file.split('_')[1])
            gt_relevant_tables = set(df[df['wikipage_id']==wikipage_id]['tables'].values[0])
           
            # Read scores
            with open(scores_path + file + '/search_output/filenameToScore.json', 'r') as fp:
                scored_tables_json = json.load(fp)['scores']

            # Ensure there are k or more scored tables in the 'scored_tables_json'
            assert len(scored_tables_json)>=k, 'There are less than k tables that have been scored for wikipage_id: ' + str(wikipage_id)

            # Get top-k tables and their scores
            top_k_tables_dict = {}
            for i in range(k):
                dict_tmp = scored_tables_json[i]
                top_k_tables_dict[dict_tmp['tableID']]=dict_tmp['score']


            # Construct groundtruth relevance scores
            gt_relevance = []
            for table in top_k_tables_dict.keys():
                if table in gt_relevant_tables:
                    gt_relevance.append(1)
                else:
                    gt_relevance.append(0)
            num_relevant_tables = gt_relevance.count(1)

            gt_relevance = np.array([gt_relevance])
            predicted_relevance = np.array([list(top_k_tables_dict.values())])

            score = ndcg_score(gt_relevance, predicted_relevance)
            scores_dict[wikipage_id] = {'ndcg': score, 'num_relevant_tables': num_relevant_tables}
        else:
            # Ignore this query if scores not found
            pass
        
    return scores_dict

def main(args):

    df = pd.read_pickle(args.query_df)

    scores_over_output = get_ndcg_scores_over_output(
        df=df,
        scores_path=args.scores_dir,
        k=args.topk
    )

    with open(args.output_dir + 'scores_over_output_' + str(args.topk) + '.json', 'w') as fp:
        json.dump(scores_over_output, fp, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--output_dir', help='Path to the directory where the scores .json file is saved', required=True)
    parser.add_argument('--query_df', help='Path to the queries dataframe (i.e., the list of wikipages that are evaluated)', required=True)
    parser.add_argument('--scores_dir', help='Path to the directory where the scores for each table are stored', required=True)
    parser.add_argument('--topk', type=int, default=10, help='Specifies the top-k value for which NDCG scores are evaluated')
    args = parser.parse_args()

    # Create the query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    print("\nOutput Directory:", args.output_dir)
    print("Query Dataframe:", args.query_df)
    print("Scores Directory:", args.scores_dir)
    print("Top-k:", args.topk,'\n')

    main(args)  