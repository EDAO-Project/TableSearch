import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
import json
import os
import random
import itertools

from pathlib import Path

from sklearn.metrics import ndcg_score
from tqdm import tqdm

import utils

def plot_runtime():
    labels = ['1', '2', '5', '10']
    types = [0, 0, 0, 0]
    embeddings = [24, 54, 141, 276]
    baseline = [49, 0, 0, 0]

    x = np.arange(len(labels))
    width = 0.75

    fig, ax = plt.subplots()
    rects_types = ax.bar(x - width / 3, types, width / 3, label = 'LSH of types')
    rects_embeddings = ax.bar(x, embeddings, width / 3, label = 'LSH of embeddings')
    rescts_baseline = ax.bar(x + width / 3, baseline, width / 3, label = 'Pure brute-force')

    ax.set_xlabel('# query tuples')
    ax.set_ylabel('Runtime (s)')
    ax.set_xticks(x, labels)
    ax.legend()

    fig.tight_layout()
    fig.savefig('runtime.pdf')
    fig.clf()

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
        filepath = Path(scores_path+file+'/filenameToScore.json')
        if filepath.is_file():
            wikipage_id = int(file.split('_')[1])

            # Set k to evaluate the query (k is set to k_multiplier times the truly relevant tables)
            num_tables = len(df[df['wikipage_id']==wikipage_id]['tables'].values[0])

            # Read scores
            with open(scores_path + file + '/filenameToScore.json', 'r') as fp:
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



def get_scores_over_output(full_df, query_df, scores_path, groundtruth_relevance_scores_dir, tables_list,
    remove_query_tables_from_evaluation_mode=None, k=10):
    '''
    Compute evaluation metrics for every query scored in the `scores_path`
    The following measures are computed/logged:
        * Number of relevant tables @k
        * Precision@k
        * Recall@k
        * NDCG@k
        * AUC score
    The NDCG score is computed over the top-k results from the output of each query
    If `remove_query_tables_from_evaluation_mode` is specified then query tables are not evaluated and removed from the groundtruth
    Return a dictionary indexed by wikipage_id that maps to a dictionary specifying the
    various evaluation metrics computed
    '''

    # Maps a wikipage_id to its respective ndcg score
    scores_dict = {}

    # Loop over each query output and compute a top-k NDCG score
    for file in tqdm(sorted(os.listdir(scores_path))):
        filepath = Path(scores_path+file+'/filenameToScore.json')
        if filepath.is_file():
            wikipage_id = int(file.split('_')[1])

            filtered_tables_list = tables_list
            # Update the `filtered_tables_list` if `remove_query_tables_from_evaluation_mode` is specified
            if remove_query_tables_from_evaluation_mode:
                filtered_tables_list, tables_removed = utils.filter_tables_list(
                    query_df=query_df, tables_list=tables_list,
                    mode=remove_query_tables_from_evaluation_mode, wikipage_id=wikipage_id
                )

            # Get the gt_to_relevance_scores_dict
            gt_tables_to_relevance_scores_dict = utils.evaluation_helpers.get_gt_tables_to_relevance_scores_dict(
               full_df=full_df, wikipage_id=wikipage_id,
               groundtruth_relevance_scores_dir=groundtruth_relevance_scores_dir, tables_list=filtered_tables_list
            )

            # Read scores and populate table_id_to_pred_score
            with open(scores_path + file + '/filenameToScore.json', 'r') as fp:
                scored_tables_json = json.load(fp)['scores']
            table_id_to_pred_score = {table_dict['tableID']:table_dict['score'] for table_dict in scored_tables_json}

            # Ensure there are k or more scored tables in the 'scored_tables_json'
            assert len(scored_tables_json)>=k, 'There are less than k tables that have been scored for wikipage_id: ' + str(wikipage_id)


            # Construct the predicted relevance scores by querying the predicted score
            # for all tables specified in the `gt_to_relevance_scores_dict`.
            # A table not found in the `table_id_to_pred_score` dictionary is set to a default score of 0
            pred_tables_to_relevance_scores_dict = {table:0 for table in gt_tables_to_relevance_scores_dict}
            for table in table_id_to_pred_score:
                pred_tables_to_relevance_scores_dict[table] = table_id_to_pred_score[table]

            # Compute the number of relevant tables in the top-k of the `table_id_to_pred_score` dictionary
            num_relevant_tables_at_k = utils.evaluation_helpers.get_num_relevant_tables_at_k(gt_tables_to_relevance_scores_dict, table_id_to_pred_score, k)

            # Compute the precision and recall @k
            precision_at_k=utils.evaluation_helpers.get_precision_at_k(num_relevant_tables_at_k, k)
            recall_at_k=utils.evaluation_helpers.get_recall_at_k(gt_tables_to_relevance_scores_dict, num_relevant_tables_at_k)

            # Compute the NDCG@k score
            gt_relevance = np.array([list(gt_tables_to_relevance_scores_dict.values())])
            predicted_relevance = np.array([list(pred_tables_to_relevance_scores_dict.values())])
            score_ndcg = ndcg_score(gt_relevance, predicted_relevance, k=k)

            # Compute AUC score (using AUC formula from: https://arxiv.org/pdf/1912.02263.pdf)
            auc_score = utils.evaluation_helpers.get_AUC_score(
                gt_tables_to_relevance_scores_dict=gt_tables_to_relevance_scores_dict,
                table_id_to_pred_score=table_id_to_pred_score
            )

            # Update the `scores_dict`
            scores_dict[wikipage_id] = {
                'ndcg': score_ndcg, 'num_relevant_tables_at_k': num_relevant_tables_at_k,
                "auc": auc_score, "precision_at_k": precision_at_k, "recall_at_k": recall_at_k
            }
        else:
            # Ignore this query if scores not found
            pass

    return scores_dict

def update_df(df, scores_path):
    '''
    Given a wikipages queries dataframe `df` and its respective `scores_path`
    update df to map the various scores @k for each wikipage
    '''
    for file in os.listdir(scores_path):
        with open(scores_path + file, 'r') as f:
            scores_dict = json.load(f)

        wikipage_id_to_ndcg = {}
        wikipage_id_to_auc = {}
        wikipage_id_to_precision = {}
        wikipage_id_to_recall = {}

        # Loop over all wikipages in the `scores_dict`
        for wikipage_id in scores_dict:
            ndcg = scores_dict[wikipage_id]['ndcg']
            auc = scores_dict[wikipage_id]['auc']
            precision = scores_dict[wikipage_id]['precision_at_k']
            recall = scores_dict[wikipage_id]['recall_at_k']

            wikipage_id_to_ndcg[int(wikipage_id)] = ndcg
            wikipage_id_to_auc[int(wikipage_id)] = auc
            wikipage_id_to_precision[int(wikipage_id)] = precision
            wikipage_id_to_recall[int(wikipage_id)] = recall

        k = file.split('_')[-1].split('.')[0]
        df['ndcg@'+k] = np.nan
        df['auc@'+k] = np.nan
        df['precision@'+k] = np.nan
        df['recall@'+k] = np.nan

        df['ndcg@'+k] = df['wikipage_id'].map(wikipage_id_to_ndcg)
        df['auc@'+k] = df['wikipage_id'].map(wikipage_id_to_auc)
        df['precision@'+k] = df['wikipage_id'].map(wikipage_id_to_precision)
        df['recall@'+k] = df['wikipage_id'].map(wikipage_id_to_recall)

    return df

def get_query_df_dict(query_df_base_dir, score_paths_base_dir, min_tuple_width, mode, tuples_per_query_list):
    # Dictionary keyed by the number of tuples per query to the query dataframe that contains the NDCG scores at various k values
    query_df_dict = {}

    for tuples_per_query in tuples_per_query_list:
        df = pd.read_pickle(query_df_base_dir+'minTupleWidth_2_tuplesPerQuery_'+str(tuples_per_query)+'.pickle')
        scores_path = score_paths_base_dir + 'minTupleWidth_' + str(min_tuple_width) + '_tuplesPerQuery_' + str(tuples_per_query) + '/' + mode + '/'

        query_df_dict[tuples_per_query] = update_df(df, scores_path)

    return query_df_dict

def get_ndcg_scores_at_k_stats(df_dict, top_k_vals, tuples_per_query_list):
    '''
    Returns the dictionaries indexed by the tuples_per_query corresponding to the mean and standard deviation of the ndcg@k scores
    '''
    mean_ndcg_scores_at_k = {}
    std_ndcg_scores_at_k = {}
    for tuples_per_query in tuples_per_query_list:
        mean_ndcg_scores_at_k[tuples_per_query] = [df_dict[tuples_per_query]['ndcg@'+str(k)].mean() for k in top_k_vals]
        std_ndcg_scores_at_k[tuples_per_query] = [df_dict[tuples_per_query]['ndcg@'+str(k)].std() for k in top_k_vals]
    return mean_ndcg_scores_at_k, std_ndcg_scores_at_k

def get_relevant_wikipages(wikipage_id, relevance_scores_dir):
    with open(relevance_scores_dir + str(wikipage_id) + '.json') as fp:
        relevant_wikipages = json.load(fp)
    print(relevant_wikipages)

def get_wikipage_attributes(wikipage_id, wikipage_to_attributes_dict, df):
    # Convert the wikipage_id into a wikipage name
    wikipage = df[df['wikipage_id'] == wikipage_id]['wikipage'].values[0]
    wikipage = wikipage.split('/')[-1]
    print(wikipage_to_attributes_dict[wikipage])

def get_query(wikipage_id, queries_path, k=None):
    with open(queries_path + 'wikipage_' + str(wikipage_id) + '.json') as fp:
        query_dict = json.load(fp)
    for tuple in query_dict['queries']:
        print(tuple)

def get_top_k_results(wikipage_id, scores_dir, k=10):
    with open(scores_dir + 'wikipage_' + str(wikipage_id)+'/search_output/filenameToScore.json') as fp:
        scores_dicts = json.load(fp)['scores']
    top_k_wikipages = []
    for dict in scores_dicts[:k]:
        top_k_wikipages.append(dict['pgTitle'])
    return top_k_wikipages

def get_top_k_bm25(wikipage_id, bm_25_scores_dir, table_to_wikipage_id_dict, df, k=10):
    scores_df = pd.read_csv(bm_25_scores_dir, sep="\t", index_col=False, names=["wikipage_id", "query", "table_id", "rank", "score", "field"])
    scores_df = scores_df[scores_df['wikipage_id'] == wikipage_id].sort_values(by='rank')
    top_k_wikipages = []
    for idx, row in scores_df.head(k).iterrows():
        wikipage_id = table_to_wikipage_id_dict[row['table_id']]
        wikipage = df[df['wikipage_id']==wikipage_id]['wikipage'].values[0].split('/')[-1]
        top_k_wikipages.append(wikipage)
    return top_k_wikipages

def get_query_summary(wikipage_id, df, relevance_scores_dir, queries_path, wikipage_to_attributes_dict, scores_dir, bm_25_scores_dir, table_to_wikipage_id_dict, k=10):
    print("Query Constructed from Wikipedia Page:", df[df['wikipage_id'] == wikipage_id]['wikipage'].values[0])

    print("\nQuery Tuples:")
    get_query(wikipage_id, queries_path)

    print('\nWikipedia Categories of query Wikipage:')
    get_wikipage_attributes(wikipage_id, wikipage_to_attributes_dict, df)

    print('\nRelevant Wikipages (Ground Truth):')
    get_relevant_wikipages(wikipage_id, relevance_scores_dir)

    print("\nTop-" + str(k), "tables found using Adj. Jaccard (Unweighted + Avg. Similarity per Column):")
    print(get_top_k_results(wikipage_id=wikipage_id, scores_dir=scores_dir, k=k))

    print("\nTop-" + str(k), "tables found using Adj. Jaccard (Weighted + Avg. Similarity per Column):")
    print(get_top_k_results(wikipage_id=wikipage_id, scores_dir=scores_dir+'weighted/avg_similarity_per_col/', k=k))

    print("\nTop-" + str(k), "tables found using Adj. Jaccard (Weighted + Max Similarity per Column):")
    print(get_top_k_results(wikipage_id=wikipage_id, scores_dir=scores_dir+'weighted/max_similarity_per_col/', k=k))

    print("\nTop-" + str(k), "tables found using BM25 (Entity Queries):")
    print(get_top_k_bm25(wikipage_id=wikipage_id, bm_25_scores_dir=bm_25_scores_dir+'entities/content.txt', 
        table_to_wikipage_id_dict=table_to_wikipage_id_dict, df=df, k=k))

    print("\nTop-" + str(k), "tables found using BM25 (Text Queries):")
    print(get_top_k_bm25(wikipage_id=wikipage_id, bm_25_scores_dir=bm_25_scores_dir+'text/content.txt', 
        table_to_wikipage_id_dict=table_to_wikipage_id_dict, df=df, k=k))

def plot_ndcg(query_tuples):
    queries_df_dir = '../../data/cikm/SemanticTableSearchDataset/queries/'

plot_runtime()
plot_ndcg(1)
plot_ndcg(2)
plot_ndcg(5)
plot_ndcg(10)
