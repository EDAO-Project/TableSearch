import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
import json
import os
import pickle
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

    ax.set_title('Top-K = 100')
    ax.set_xlabel('# query tuples')
    ax.set_ylabel('Runtime (s)')
    ax.set_xticks(x, labels)
    ax.legend()

    fig.tight_layout()
    fig.savefig('runtime.pdf')
    fig.clf()

# Returns: query, [relevance score, table ID]
def ground_truth(query_filename, ground_truth_folder, table_corpus_folder, pickle_mapping_file):
    query = None
    relevances = list()
    wikipages = None
    mapping = None

    with open(query_filename, 'r') as file:
        query = json.load(file)['queries']

    with open(ground_truth_folder + '/' + query_filename.split('/')[-1].split('_')[1], 'r') as file:
        wikipages = json.load(file)

    with open(pickle_mapping_file, 'rb') as file:
        mapping = pickle.load(file)

    for key, value in wikipages.items():
        table_folders = os.listdir(table_corpus_folder)
        wikipage = 'https://en.wikipedia.org/wiki/' + key
        tables = None

        for key in mapping['wikipage'].keys():
            if wikipage == mapping['wikipage'][key]:
                tables = mapping['tables'][key]
                break

        if (tables == None):
            tables = []

        for table in tables:
            for table_folder in table_folders:
                if '.' in table_folder:
                    continue

                table_files = os.listdir(table_corpus_folder + '/' + table_folder)

                if table in table_files:
                    relevances.append([value, table])

    return query, relevances

# Predicted tables not among among ground truth are given a score 0
def predicted_scores(query_id, tables, mode, buckets, tuples, k):
    path = 'results/' + mode + '/buckets_' + str(buckets) + '/' + str(tuples) + '_tuple_queries/' + str(k) + '/search_output/' + query_id + '/filenameToScore.json'
    tables = json.load(path)
    scores = list()

    for table in tables['scores']:
        scores.append(float(table['score']))

    return scores

# Returns map: ['types'|'embeddings'|'baseline']->[<# BUCKETS: [150|300]>]->[<TOP-K: [10|50|100]>]->
def plot_ndcg(query_tuples):
    query_dir = '../../data/cikm/SemanticTableSearchDataset/queries/' + str(query_tuples) + '_tuples_per_query/'
    ground_truth_dir = '../../data/cikm/SemanticTableSearchDataset/ground_truth/wikipedia_categories'
    corpus = '../../data/cikm/SemanticTableSearchDataset/table_corpus/tables'
    mapping_file = '../../data/cikm/SemanticTableSearchDataset/table_corpus/wikipages_df.pickle'
    query_files = os.listdir(query_dir)
    top_k = [10, 100]
    buckets = [150, 300]
    ndcg = dictionary()
    ndcg['types'] = dictionary()
    ndcg['embeddings' = dictionary()
    ndcg['baseline'] = dictionary()

    for b in buckets:
        ndcg['types'][b] = dictionary()
        ndcg['embeddings'][b] = dictionary()
        ndcg['baseline'][b] = dictionary()

        for k in top_k:
            ndcg['types'][b][k] = list()
            ndcg['embeddings'][b][k] = list()
            ndcg['baseline'][b][k] = list()

            for query_file in query_files:
                query_id = query_file.split('.')[0]
                query_path = query_dir + query_file
                truth_embeddings = ground_truth(query_path, ground_truth_dir, corpus, mapping_file)
                print(str(truth_embeddings))

                gt_relevance = list()
                gt_tables = list()

                for relevance in truth_embeddings[1]:
                    gt_relevance.append(relevance[0])
                    gt_tables.append(relevance[1])

                # Types
                predicted_relevance = None

                # Embeddings
                predicted_relevance = predicted_scores(query_id, gt_tables, 'embeddings', b, query_tuples, k)
                ndcg_embeddings = ndcg_score(gt_relevance, predicted_relevance, k = k)
                ndcg['embeddings'][b][k].append(ndcg_embeddings)

                # Baseline
                predicted_relevance = list()

#plot_runtime()
plot_ndcg(1)
#plot_ndcg(2)
#plot_ndcg(5)
#plot_ndcg(10)
