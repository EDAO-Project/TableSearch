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
    labels = ['1', '2', '5']
    types = [0, 0, 0]
    embeddings = [24, 54, 141]
    baseline = [49, 82, 167]

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

# Returns None if results for given query ID do not exist
def predicted_scores(query_id, mode, vectors, k, gt_tables):
    path = 'results/' + mode + '/vectors_' + str(vectors) + '/' + str(k) + '/search_output/' + query_id + '/filenameToScore.json'

    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        tables = json.load(f)
        predicted = dict()

        for table in tables['scores']:
            predicted[table['tableID']] = table['score']

        scores = {table:0 for table in gt_tables}

        for table in predicted:
            scores[table] = predicted[table]

        return list(scores.values())

def full_corpus(base_dir):
    files = os.listdir(base_dir)
    tables = list()

    for file in files:
        tables.append(file)

    return tables

def gen_boxplots(ndcg_dict):
    labels = ['T@10', 'T@100', 'E@10', 'E@100', 'B@10', 'B@100']
    colors = ['lightblue', 'blue', 'lightgreen', 'green', 'pink', 'red']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (9, 4))
    scores_32_vectors = list()
    scores_64_vectors = list()

    scores_32_vectors.append(ndcg_dict['types']['32']['10'])
    scores_32_vectors.append(ndcg_dict['types']['32']['100'])
    scores_32_vectors.append(ndcg_dict['embeddings']['32']['10'])
    scores_32_vectors.append(ndcg_dict['embeddings']['32']['100'])
    scores_32_vectors.append(ndcg_dict['baseline_jaccard']['32']['10'])
    scores_32_vectors.append(ndcg_dict['baseline_jaccard']['32']['100'])
    scores_32_vectors.append(ndcg_dict['baseline_cosine']['32']['100'])
    scores_32_vectors.append(ndcg_dict['baseline_cosine']['32']['100'])

    scores_64_vectors.append(ndcg_dict['types']['64']['10'])
    scores_64_vectors.append(ndcg_dict['types']['64']['100'])
    scores_64_vectors.append(ndcg_dict['embeddings']['64']['10'])
    scores_64_vectors.append(ndcg_dict['embeddings']['64']['100'])
    scores_64_vectors.append(ndcg_dict['baseline_jaccard']['32']['10'])
    scores_64_vectors.append(ndcg_dict['baseline_jaccard']['32']['100'])
    scores_64_vectors.append(ndcg_dict['baseline_cosine']['32']['100'])
    scores_64_vectors.append(ndcg_dict['baseline_cosine']['32']['100'])

    plot_32_vectors = ax1.boxplot(scores_32_vectors, vert = True, patch_artist = True, labels = labels)
    ax1.set_title('32 permutation/projection')

    plot_64_vectors = ax2.boxplot(scores_64_vectors, vert = True, patch_artist = True, labels = labels)
    ax2.set_title('64 permutation/projection')

    for plot in (plot_32_vectors, plot_64_vectors):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    ax1.set_xlabel('Approach@K (T = LSH of types, E = LSH of embeddings, B = Brute-Force')

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')

    plt.savefig('debug_plot.pdf', format = 'pdf')
    plt.clf()

# Returns map: ['types'|'embeddings'|'baseline']->[<# BUCKETS: [150|300]>]->[<TOP-K: [10|100]>]->[NDCG SCORES]
def plot_ndcg():
    query_dir = 'queries/'
    ground_truth_dir = '../../data/cikm/SemanticTableSearchDataset/ground_truth/wikipedia_categories'
    corpus = 'tables'
    mapping_file = '../../data/cikm/SemanticTableSearchDataset/table_corpus/wikipages_df.pickle'
    query_files = os.listdir(query_dir)
    table_files = full_corpus(corpus + '/redirect')
    top_k = [10, 100]
    vectors = [32, 64]
    ndcg = dict()
    ndcg['types'] = dict()
    ndcg['embeddings'] = dict()
    ndcg['baseline'] = dict()

    for v in vectors:
        ndcg['types'][str(v)] = dict()
        ndcg['embeddings'][str(v)] = dict()
        ndcg['baseline']['32'] = dict()

        for k in top_k:
            ndcg['types'][str(v)][str(k)] = list()
            ndcg['embeddings'][str(v)][str(k)] = list()
            ndcg['baseline']['32'][str(k)] = list()

            count = 0
            print('K = ' + str(k) + ', vectors = ' + str(v))

            for query_file in query_files:
                count += 1

                query_id = query_file.split('.')[0]
                query_path = query_dir + query_file
                truth = ground_truth(query_path, ground_truth_dir, corpus, mapping_file)
                gt_rels = {table:0 for table in table_files}

                for relevance in truth[1]:
                    gt_rels[relevance[1]] = relevance[0]

#                print(gt_rels)

                # Types
                predicted_relevance = predicted_scores(query_id, 'types', v, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['types'][str(v)][str(k)].append(ndcg_types)

                # Embeddings
                predicted_relevance = predicted_scores(query_id, 'embeddings', v, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['embeddings'][str(v)][str(k)].append(ndcg_embeddings)

                # Baseline - get only for 150 buckets
                predicted_relevance = predicted_scores(query_id, 'baseline', 32, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['32'][str(k)].append(ndcg_baseline)

                if count == 100:
                    break

    gen_boxplots(ndcg)

#plot_runtime()
plot_ndcg()
