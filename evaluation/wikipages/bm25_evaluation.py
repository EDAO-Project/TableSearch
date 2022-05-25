import pandas as pd
import numpy as np
import json
import os
import operator
import pickle


import utils
import argparse


from pathlib import Path

from sklearn.metrics import ndcg_score
from tqdm import tqdm

# query_df_base_dir = '../../data/queries/wikipages/query_dataframes/wikipages_test_dataset/filtered_queries/'
min_tuple_width=2
tuples_per_query_list = [1, 2, 5, 10]
top_k_vals = [5,10,15,20,50,100,150,200]


def get_bm25_ndcg_scores_over_output(args, full_df, tables_list, remove_query_tables_from_evaluation_mode=None):
    '''
    Computes the ndcg scores and returns the dfs_dict
    '''

    # Dictionary keyed by tuples per query and maps to another dictionary keyed by 'entities' and 'text'
    # which in turn map to a dictionary keyed by 'catchall', 'content', etc. and map to their respective dataframes with NDCG scores
    dfs_dict = {}

    for tuples_per_query in tqdm(tuples_per_query_list):
        base_num_tuples_path='minTupleWidth_' + str(min_tuple_width) + '_tuplesPerQuery_'+str(tuples_per_query)
        bm25_scores_path_entities = args.scores_dir+base_num_tuples_path+'/entities/'
        bm25_scores_path_text = args.scores_dir+base_num_tuples_path+'/text/'

        # Read the query dataframe
        query_df = pd.read_pickle(args.query_dfs_dir + base_num_tuples_path+'.pickle')

        # Construct the modified dataframes with the NDCG scores

        # # Text Queries
        # df_content_text_categories = utils.evaluation_helpers.get_updated_df(
        #     query_df=query_df.copy(), full_df=full_df, scores_path=bm25_scores_path_text+'content.txt',
        #     k_vals=top_k_vals, tables_list=tables_list, groundtruth_relevance_scores_dir=args.groundtruth_relevance_scores_dir,
        #     remove_query_tables_from_evaluation_mode=args.remove_query_tables_from_evaluation_mode
        # )
        # print("Finished text query with categories for", tuples_per_query, "tuples per query")

        # Entity Queries
        df_content_entities_categories = utils.evaluation_helpers.get_updated_df(
            query_df=query_df.copy(), full_df=full_df, scores_path=bm25_scores_path_entities+'content.txt',
            k_vals=top_k_vals, tables_list=tables_list, groundtruth_relevance_scores_dir=args.groundtruth_relevance_scores_dir,
            remove_query_tables_from_evaluation_mode=args.remove_query_tables_from_evaluation_mode
        )
        print("Finished entity query with navigation links for", tuples_per_query, "tuples per query")

        # Update dfs_dict
        dfs_dict[tuples_per_query] = {}
        dfs_dict[tuples_per_query]['entities'] = {'content_categories': df_content_entities_categories, 'content_navigation_links': []}
        # dfs_dict[tuples_per_query]['text'] = {'content_categories': df_content_text_categories,'content_navigation_links': []} 

    return dfs_dict

def main(args):
    full_df = pd.read_pickle(args.full_df)

    # Extract the names of all tables in our search space
    tables_list = os.listdir(args.tables_dir)

    dfs_dict = get_bm25_ndcg_scores_over_output(
        args=args,
        full_df=full_df,
        tables_list=tables_list,
        remove_query_tables_from_evaluation_mode=args.remove_query_tables_from_evaluation_mode
    )

    with open(args.output_df_path, 'wb') as handle:
        pickle.dump(dfs_dict, handle)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--output_df_path', help='Path to the output df pickle file to be generated', required=True)
    parser.add_argument('--query_dfs_dir', help='Path to the directory with the query dataframes (i.e., the list of wikipages that are evaluated)', required=True)
    parser.add_argument('--scores_dir', help='Path to the directory where the bm25 scores', required=True)
    parser.add_argument('--full_df', help='Path to the full queries dataframe (i.e., it provides mappings for each wikipage to its set of tables', required=True)
    parser.add_argument('--groundtruth_relevance_scores_dir', help='Path to the directory that contains the groundtruth relevance scores for each wikipage', required=True)
    parser.add_argument('--tables_dir', help='Path to the directory containing all the tables that make up the search space for all queries', required=True)
    
    parser.add_argument('--remove_query_tables_from_evaluation_mode', choices=['remove_query_table', 'remove_query_wikipage_tables'], 
        help='If specified then the query wikitable or all tables found in the query wikipage (depending on which mode was specified) \
        are removed from the evaluation')
    args = parser.parse_args()

    print("\nOutput Dataframe Path:", args.output_df_path)
    print("Query Dataframe Directory:", args.query_dfs_dir)
    print("Scores Directory:", args.scores_dir)
    print("Full Dataframe:", args.full_df)
    print("Tables Directory:", args.tables_dir)
    if args.remove_query_tables_from_evaluation_mode:
        print('Remove Query Tables from Evaluation Mode:', args.remove_query_tables_from_evaluation_mode)
    print('\n')

    main(args) 