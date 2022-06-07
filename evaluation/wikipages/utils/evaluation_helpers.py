import pandas as pd
import numpy as np
import json

from sklearn.metrics import ndcg_score
from scipy.stats import rankdata
from tqdm import tqdm

def get_gt_tables_to_relevance_scores_dict(full_df, wikipage_id, groundtruth_relevance_scores_dir, tables_list):
    '''
    Return a dictionary mapping each table_id to its relevance score

    Scores for only the tables in `tables_list` are returned in the dictionary
    '''

    gt_tables_to_relevance_scores_dict = {table:0 for table in tables_list}

    # Get the set of relevant wikipages and their 
    with open(groundtruth_relevance_scores_dir + str(wikipage_id) + '.json', 'r') as fp:
        gt_relevance = json.load(fp)
   
    # Extract the tables from each relevant wikipage
    for wikipage in gt_relevance:
        if ('https://en.wikipedia.org/wiki/' + wikipage) in full_df['wikipage'].values:
            wikipage_tables = full_df[full_df['wikipage'] == 'https://en.wikipedia.org/wiki/' + wikipage]['tables'].tolist()[0]
            relevance_score = gt_relevance[wikipage]

            for table in wikipage_tables:
                if table in gt_tables_to_relevance_scores_dict:
                    # Update the score of existing table in the dictionary because a higher relevance for the same table was discovered
                    if relevance_score > gt_tables_to_relevance_scores_dict[table]:
                        gt_tables_to_relevance_scores_dict[table] = relevance_score

    return gt_tables_to_relevance_scores_dict

def get_scores_bm25(query_df, full_df, scores_df, tables_list, k=10,
    groundtruth_relevance_scores_dir=None, remove_query_tables_from_evaluation_mode=None):
    '''
    Returns a dictionary of dictionaries mapping each evaluation measure to a dictionary keyed by the wikipage_id
    and mapping to evaluation measures
    '''

    # Initialize dictionaries 
    wikipage_id_to_ndcg_score = {}
    wikipage_id_to_num_relevant_tables_at_k = {}
    wikipage_id_to_precision_at_k = {}
    wikipage_id_to_recall_at_k = {}
    wikipage_id_to_auc_score = {}

    for wikipage_id in tqdm(query_df['wikipage_id']):
        # Update the `filtered_tables_list` if `remove_query_tables_from_evaluation_mode` is specified
        filtered_tables_list = tables_list
        if remove_query_tables_from_evaluation_mode:
            filtered_tables_list, tables_removed = filter_tables_list(
                query_df=query_df, tables_list=tables_list,
                mode=remove_query_tables_from_evaluation_mode, wikipage_id=wikipage_id
            )
  
        # Get the ground truth relevance scores for all marked tables
        gt_tables_to_relevance_scores_dict = get_gt_tables_to_relevance_scores_dict(
            full_df=full_df, wikipage_id=wikipage_id,
            groundtruth_relevance_scores_dir=groundtruth_relevance_scores_dir,
            tables_list=filtered_tables_list
        )

        cur_df = scores_df[scores_df['wikipage_id']==wikipage_id]

        # Get the top-k tables from cur_df and their scores
        if len(cur_df.index) >= k:
            table_id_to_pred_score = pd.Series(cur_df['score'].values,index=cur_df['table_id']).to_dict()

            # Construct groundtruth relevance scores
            gt_relevance = list(gt_tables_to_relevance_scores_dict.values())

            # Construct the predicted relevance scores
            pred_tables_to_relevance_scores_dict = {table:0 for table in gt_tables_to_relevance_scores_dict}
            for table in gt_tables_to_relevance_scores_dict:
                if table in table_id_to_pred_score:
                    pred_tables_to_relevance_scores_dict[table] = table_id_to_pred_score[table]

            # Compute the number of relevant tables in the top-k of the `table_id_to_pred_score` dictionary
            num_relevant_tables_at_k = get_num_relevant_tables_at_k(gt_tables_to_relevance_scores_dict, table_id_to_pred_score, k)

            # Compute the precision and recall @k
            precision_at_k=get_precision_at_k(num_relevant_tables_at_k, k)
            recall_at_k=get_recall_at_k(gt_tables_to_relevance_scores_dict, num_relevant_tables_at_k)

            # Compute the NDCG@k score 
            gt_relevance = np.array([gt_relevance])
            predicted_relevance = np.array([list(pred_tables_to_relevance_scores_dict.values())])
            score_ndcg = ndcg_score(gt_relevance, predicted_relevance, k=k)

            # Compute AUC score (using AUC formula from: https://arxiv.org/pdf/1912.02263.pdf)
            auc_score = get_AUC_score(
                gt_tables_to_relevance_scores_dict=gt_tables_to_relevance_scores_dict,
                table_id_to_pred_score=table_id_to_pred_score
            )

            # Save the scores in their respective dictionaries
            wikipage_id_to_num_relevant_tables_at_k[int(wikipage_id)] = num_relevant_tables_at_k
            wikipage_id_to_precision_at_k[int(wikipage_id)] = precision_at_k
            wikipage_id_to_recall_at_k[int(wikipage_id)] = recall_at_k
            wikipage_id_to_ndcg_score[int(wikipage_id)] = score_ndcg
            wikipage_id_to_auc_score[int(wikipage_id)] = auc_score


    evaluation_scores_dict= {
        "ndcg": wikipage_id_to_ndcg_score,
        "num_relevant_tables_at_k": wikipage_id_to_num_relevant_tables_at_k,
        "auc": wikipage_id_to_auc_score,
        "precision_at_k": wikipage_id_to_precision_at_k,
        "recall_at_k": wikipage_id_to_recall_at_k
    }

    return evaluation_scores_dict

def filter_tables_list(query_df, tables_list, mode, wikipage_id):
    '''
    Returns an updated `tables_list` based on the specified `mode` of removing query tables from the evaluation
    as well as a list of the tables removed
    '''
    row = query_df[query_df['wikipage_id']==wikipage_id]
    tables_to_remove = []
    if mode == 'remove_query_table':
        tables_to_remove.extend(row['selected_table'])
    elif mode == 'remove_query_wikipage_tables':
        [tables_to_remove.append(table) for table in row['tables'].to_list()[0]]

    # Remove all `tables_to_remove` from `tables_list`
    tables_list = [table for table in tables_list if table not in tables_to_remove]

    return tables_list, tables_to_remove

def get_updated_df(query_df, full_df, scores_path, k_vals, tables_list,
    groundtruth_relevance_scores_dir=None, remove_query_tables_from_evaluation_mode=None):
    '''
    Updates the `query_df` by adding ndcg scores at the various specified `k_vals`
    '''
    scores_df = pd.read_csv(scores_path, sep="\t", index_col=False, names=["wikipage_id", "query", "table_id", "rank", "score", "field"])

    for k in tqdm(k_vals):
        wikipage_id_to_score_dict = get_scores_bm25(
            query_df=query_df, full_df=full_df, scores_df=scores_df, k=k, tables_list=tables_list,
            groundtruth_relevance_scores_dir=groundtruth_relevance_scores_dir,
            remove_query_tables_from_evaluation_mode=remove_query_tables_from_evaluation_mode
        )

        # Initialize the dataframe columns
        query_df['ndcg@'+str(k)] = np.nan
        query_df['num_relevant_tables@'+str(k)] = np.nan
        query_df['auc@'+str(k)] = np.nan
        query_df['precision@'+str(k)] = np.nan
        query_df['recall@'+str(k)] = np.nan

        # Populate the dataframe columns from the `wikipage_id_to_score_dict``
        query_df['ndcg@'+str(k)] = query_df['wikipage_id'].map(wikipage_id_to_score_dict['ndcg'])
        query_df['num_relevant_tables@'+str(k)] = query_df['wikipage_id'].map(wikipage_id_to_score_dict['num_relevant_tables_at_k'])
        query_df['auc@'+str(k)] = query_df['wikipage_id'].map(wikipage_id_to_score_dict['auc'])
        query_df['precision@'+str(k)] = query_df['wikipage_id'].map(wikipage_id_to_score_dict['precision_at_k'])
        query_df['recall@'+str(k)] = query_df['wikipage_id'].map(wikipage_id_to_score_dict['recall_at_k'])

    return query_df

def get_precision_at_k(num_relevant_tables_at_k, k):
    return num_relevant_tables_at_k / k

def get_recall_at_k(gt_tables_to_relevance_scores_dict, num_relevant_tables_at_k):
    relevant_tables_gt = [table for table in gt_tables_to_relevance_scores_dict if gt_tables_to_relevance_scores_dict[table]>0]
    return num_relevant_tables_at_k/len(relevant_tables_gt)

def get_num_relevant_tables_at_k(gt_tables_to_relevance_scores_dict, table_id_to_pred_score, k):
    relevant_tables_gt = set([table for table in gt_tables_to_relevance_scores_dict if gt_tables_to_relevance_scores_dict[table]>0])
    num_relevant_tables = 0
    for table in list(table_id_to_pred_score.keys())[:k]:
        if table in relevant_tables_gt:
            num_relevant_tables+=1
    
    return num_relevant_tables


def get_average_rank_of_zero_scored_tables(num_tables, num_scored_tables):
    '''
    Compute the average rank as the arithmetic sum from (num_scored_tables+1) to num_tables and dividing
    by (num_tables-num_scored_tables) (i.e. the average of the terms in the summation)
    '''
    arithmetic_sum = ((num_tables-num_scored_tables)*(num_scored_tables+1+num_tables)) / 2
    average_rank = arithmetic_sum / (num_tables-num_scored_tables)
    return average_rank


def get_AUC_score(gt_tables_to_relevance_scores_dict, table_id_to_pred_score):
    '''
    Computes the AUC score for a given query given the `gt_tables_to_relevance_scores_dict` and the `table_id_to_pred_score`

    For details on the AUC formula see: https://arxiv.org/pdf/1912.02263.pdf
    '''

    n = len(gt_tables_to_relevance_scores_dict)

    # Extract the set of all ground truth relevant tables
    relevant_tables = [table for table in gt_tables_to_relevance_scores_dict if gt_tables_to_relevance_scores_dict[table]>0]
    num_relevant_tables = len(relevant_tables)

    # Construct a dictionary mapping to the ranks of all tables in the `table_id_to_pred_score` dictionary
    # In case of ties the average is used (ranking is done in descending order, so highest scored table gets the rank 1)
    table_to_rank_dict = dict(zip(table_id_to_pred_score.keys(), rankdata([-i for i in table_id_to_pred_score.values()], method='average')))
    
    # Compute the AUC score
    relevant_tables_rank_sum = 0
    for table in relevant_tables:
        if table in table_to_rank_dict:
            relevant_tables_rank_sum += table_to_rank_dict[table]
        else:
            # Current table not found in the top-k scored tables in `table_id_to_pred_score`, 
            # so we assume a zero score and the rank is the average rank of all zero scored tables.
            # The average rank is the average of the arithmetic sum from len(table_to_rank_dict) to n
            average_rank = get_average_rank_of_zero_scored_tables(num_tables=n, num_scored_tables=len(table_to_rank_dict))
            relevant_tables_rank_sum += average_rank

    auc_score = (n - (num_relevant_tables-1)/2 - relevant_tables_rank_sum/num_relevant_tables) / (n-num_relevant_tables)
    return auc_score
