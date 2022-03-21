import pandas as pd
import numpy as np
import json

from sklearn.metrics import ndcg_score
from tqdm import tqdm

def get_gt_tables_to_relevance_scores_dict(full_df, wikipage_id, groundtruth_relevance_scores_dir, tables_list):
    '''
    Return a dictionary mapping each table_id to its relevance score
    '''

    gt_tables_to_relevance_scores_dict = {table:0 for table in tables_list}

    # Get the set of relevant wikipages and their 
    with open(groundtruth_relevance_scores_dir + str(wikipage_id) + '.json', 'r') as fp:
        gt_relevance = json.load(fp)
    
    # Extract the tables from each relevant wikipage
    for wikipage in gt_relevance:
        if ('https://en.wikipedia.org/wiki/' + wikipage) in full_df['wikipage']:
            wikipage_tables = full_df[full_df['wikipage'] == 'https://en.wikipedia.org/wiki/' + wikipage]['tables'].tolist()[0]
            relevance_score = gt_relevance[wikipage]
        
            for table in wikipage_tables:
                if table in gt_tables_to_relevance_scores_dict:
                    # Update the score of existing table in the dictionary because a higher relevance for the same table was discovered
                    if relevance_score > gt_tables_to_relevance_scores_dict[table]:
                        gt_tables_to_relevance_scores_dict[table] = relevance_score
                else:
                    gt_tables_to_relevance_scores_dict[table] = relevance_score

    return gt_tables_to_relevance_scores_dict

def get_ndcg_scores_bm25(full_df, scores_df, tables_list, k=10, groundtruth_relevance_scores_dir=None):
    wikipage_id_to_ndcg_score = {}

    for wikipage_id in scores_df['wikipage_id'].unique():
        # Get the ground truth relevance scores for all marked tables
        gt_tables_to_relevance_scores_dict = get_gt_tables_to_relevance_scores_dict(
            full_df, wikipage_id, groundtruth_relevance_scores_dir, tables_list
        )

        cur_df = scores_df[scores_df['wikipage_id']==wikipage_id]

        # Get the top-k tables from cur_df and their scores
        if len(cur_df.index) >= k:
            cur_df = cur_df.head(k)
            table_id_to_pred_score = pd.Series(cur_df['score'].values,index=cur_df['table_id']).to_dict()

            # Construct groundtruth relevance scores
            gt_relevance = list(gt_tables_to_relevance_scores_dict.values())

            # Construct the predicted relevance scores
            pred_tables_to_relevance_scores_dict = {table:0 for table in gt_tables_to_relevance_scores_dict}
            for table in table_id_to_pred_score:
                # Update the pred_tables_to_relevance_scores_dict
                pred_tables_to_relevance_scores_dict[table] = table_id_to_pred_score[table]

            gt_relevance = np.array([gt_relevance])
            predicted_relevance = np.array([list(pred_tables_to_relevance_scores_dict.values())])

            score = ndcg_score(gt_relevance, predicted_relevance, k=k)
            wikipage_id_to_ndcg_score[int(wikipage_id)] = score

    return wikipage_id_to_ndcg_score

def get_updated_df(df, full_df, scores_path, k_vals, tables_list, groundtruth_relevance_scores_dir=None):
    scores_df = pd.read_csv(scores_path, sep="\t", index_col=False, names=["wikipage_id", "query", "table_id", "rank", "score", "field"])

    for k in tqdm(k_vals):
        wikipage_id_to_ndcg_score = get_ndcg_scores_bm25(
            full_df=full_df, scores_df=scores_df, k=k, tables_list=tables_list,
            groundtruth_relevance_scores_dir=groundtruth_relevance_scores_dir)
        df['ndcg@'+str(k)] = np.nan
        df['ndcg@'+str(k)] = df['wikipage_id'].map(wikipage_id_to_ndcg_score)

    return df