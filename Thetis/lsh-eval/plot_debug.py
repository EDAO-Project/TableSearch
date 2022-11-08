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
def predicted_scores(query_id, votes, mode, vectors, band_size, k, gt_tables, is_baseline = False, is_column_aggregation = False):
    path = 'results/vote_' + str(votes) + '/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/search_output/' + query_id + '/filenameToScore.json'

    if is_baseline:
        path = 'results/baseline/baseline_' + mode + '/' + str(k) + '/search_output/' + query_id + '/filenameToScore.json'

    elif (is_column_aggregation):
        path = 'results/aggregation/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/search_output/' + query_id + '/filenameToScore.json'

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

def gen_boxplots(ndcg_dict, votes):
    labels = ['T(V=30, BS=10)', 'T(V=128, BS=8)', 'E(V=30, BS=10)', 'E(V=128, BS=8)', 'TC(V=30, BS=10)', 'TC(V=128, BS=8)', 'EC(V=30, BS=10)', 'EC(V=128, BS=8)', 'B - Jaccard', 'B - cosine']
    colors = ['lightblue', 'blue', 'lightgreen', 'green', 'pink', 'red']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (85, 10))
    data10 = list()
    data100 = list()

    data10.append(ndcg_dict[str(votes)]['types']['30']['10']['10'])
    data10.append(ndcg_dict[str(votes)]['types']['128']['8']['10'])
    data10.append(ndcg_dict[str(votes)]['embeddings']['30']['10']['10'])
    data10.append(ndcg_dict[str(votes)]['embeddings']['128']['8']['10'])
    data10.append(ndcg_dict['1']['types_column']['30']['10']['10'])
    data10.append(ndcg_dict['1']['types_column']['128']['8']['10'])
    data10.append(ndcg_dict['1']['embeddings_column']['30']['10']['10'])
    data10.append(ndcg_dict['1']['embeddings_column']['128']['8']['10'])
    data10.append(ndcg_dict['baseline']['jaccard']['10'])
    data10.append(ndcg_dict['baseline']['cosine']['10'])

    data100.append(ndcg_dict[str(votes)]['types']['30']['10']['100'])
    data100.append(ndcg_dict[str(votes)]['types']['128']['8']['100'])
    data100.append(ndcg_dict[str(votes)]['embeddings']['30']['10']['100'])
    data100.append(ndcg_dict[str(votes)]['embeddings']['128']['8']['100'])
    data100.append(ndcg_dict['1']['types_column']['30']['10']['100'])
    data100.append(ndcg_dict['1']['types_column']['128']['8']['100'])
    data100.append(ndcg_dict['1']['embeddings_column']['30']['10']['100'])
    data100.append(ndcg_dict['1']['embeddings_column']['128']['8']['100'])
    data100.append(ndcg_dict['baseline']['jaccard']['100'])
    data100.append(ndcg_dict['baseline']['cosine']['100'])

    plot_k_10 = ax1.boxplot(data10, vert = True, patch_artist = True, labels = labels)
    ax1.set_title('K = 10')

    plot_k_100 = ax2.boxplot(data100, vert = True, patch_artist = True, labels = labels)
    ax2.set_title('K = 100')

    for plot in (plot_k_10, plot_k_100):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    ax1.set_xlabel('T = LSH of types, E = LSH of embeddings, TC = Column-based LSH of types, EC = Column-based LSH of embeddings, B = Brute-Force V = # permutation/projection vectors, BS = band size')

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')

    plt.savefig('debug_plot_' + str(votes) + '_votes.pdf', format = 'pdf')
    plt.clf()

# Returns map: ['types'|'embeddings'|'baseline']->[<# BUCKETS: [150|300]>]->[<TOP-K: [10|100]>]->[NDCG SCORES]
def plot_ndcg():
    query_dir = 'queries/'
    ground_truth_dir = '../../data/cikm/SemanticTableSearchDataset/ground_truth/wikipedia_categories'
    corpus = '/data/cikm/SemanticTableSearchDataset/table_corpus/tables'
    mapping_file = '../../data/cikm/SemanticTableSearchDataset/table_corpus/wikipages_df.pickle'
    query_files = os.listdir(query_dir)
    table_files = full_corpus(corpus + '/../corpus')
    top_k = [10, 100]
    votes = [1, 2, 3, 4]
    ndcg = dict()

    for vote in votes:
        ndcg[str(vote)] = dict()
        ndcg[str(vote)]['types'] = dict()
        ndcg['1']['types_column'] = dict()
        ndcg[str(vote)]['embeddings'] = dict()
        ndcg['1']['embeddings_column'] = dict()
        ndcg[str(vote)]['types']['30'] = dict()
        ndcg[str(vote)]['types']['128'] = dict()
        ndcg['1']['types_column']['30'] = dict()
        ndcg['1']['types_column']['128'] = dict()
        ndcg[str(vote)]['embeddings']['30'] = dict()
        ndcg[str(vote)]['embeddings']['128'] = dict()
        ndcg['1']['embeddings_column']['30'] = dict()
        ndcg['1']['embeddings_column']['128'] = dict()
        ndcg[str(vote)]['types']['30']['10'] = dict()
        ndcg[str(vote)]['types']['128']['8'] = dict()
        ndcg['1']['types_column']['30']['10'] = dict()
        ndcg['1']['types_column']['128']['8'] = dict()
        ndcg[str(vote)]['embeddings']['30']['10'] = dict()
        ndcg[str(vote)]['embeddings']['128']['8'] = dict()
        ndcg['1']['embeddings_column']['30']['10'] = dict()
        ndcg['1']['embeddings_column']['128']['8'] = dict()
        ndcg['baseline'] = dict()
        ndcg['baseline']['jaccard'] = dict()
        ndcg['baseline']['cosine'] = dict()

        for k in top_k:
            ndcg[str(vote)]['types']['30']['10'][str(k)] = list()
            ndcg[str(vote)]['types']['128']['8'][str(k)] = list()
            ndcg['1']['types_column']['30']['10'][str(k)] = list()
            ndcg['1']['types_column']['128']['8'][str(k)] = list()
            ndcg[str(vote)]['embeddings']['30']['10'][str(k)] = list()
            ndcg[str(vote)]['embeddings']['128']['8'][str(k)] = list()
            ndcg['1']['embeddings_column']['30']['10'][str(k)] = list()
            ndcg['1']['embeddings_column']['128']['8'][str(k)] = list()
            ndcg['baseline']['jaccard'][str(k)] = list()
            ndcg['baseline']['cosine'][str(k)] = list()

            count = 0
            print('K = ' + str(k) + ', Vote = ' + str(vote))

            for query_file in query_files:
                count += 1

                query_id = query_file.split('.')[0]
                query_path = query_dir + query_file
                truth = ground_truth(query_path, ground_truth_dir, corpus, mapping_file)
                gt_rels = {table:0 for table in table_files}

                for relevance in truth[1]:
                    gt_rels[relevance[1]] = relevance[0]

                # Types
                predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types']['30']['10'][str(k)].append(ndcg_types)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types']['128']['8'][str(k)].append(ndcg_types)

                # Types - column aggregation
                predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['1']['embeddings_column']['30']['10'][str(k)].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['1']['embeddings_column']['128']['8'][str(k)].append(ndcg_embeddings)

                # Embeddings
                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings']['30']['10'][str(k)].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings']['128']['8'][str(k)].append(ndcg_embeddings)

                # Embeddings - Column aggregation
                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['1']['embeddings_column']['30']['10'][str(k)].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['1']['embeddings_column']['128']['8'][str(k)].append(ndcg_embeddings)

                # Baseline
                predicted_relevance = predicted_scores(query_id, vote, 'jaccard', 32, 8, k, gt_rels, True, False)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['jaccard'][str(k)].append(ndcg_baseline)

                predicted_relevance = predicted_scores(query_id, vote, 'cosine', 32, 8, k, gt_rels, True, False)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['cosine'][str(k)].append(ndcg_baseline)

        gen_boxplots(ndcg, vote)

plot_ndcg()
