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
def predicted_scores(query_id, votes, mode, vectors, band_size, tuples, k, gt_tables, is_baseline = False, is_column_aggregation = False, is_bm25 = False, is_bm25_prefilter = False):
    path = 'results/vote_' + str(votes) + '/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    if (is_baseline):
        path = 'results/baseline/baseline_' + mode + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    elif (is_column_aggregation):
        path = 'results/vote_' + str(votes) + '/aggregation/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    if (is_bm25):
        path = 'results/baseline/bm25/' + str(k) + '/' + str(tuples) + '-tuple/' + mode + '/content.txt'

    elif (is_bm25_prefilter):
        path = 'results/baseline/baseline_bm25_prefiltering/' + mode + '/' str(k) + '/' str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        predicted = dict()

        if (is_bm25):
            for line in f:
                split = line.split('\t')
                qid = 'wikipage_' + split[0]
                table = split[2]
                score = float(split[4])

                if (qid == query_id):
                    predicted[table] = score

        else:
            tables = json.load(f)

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

def gen_boxplots(ndcg_dict, votes, tuples, k):
    # First the plots of all configurations

    plt.rc('xtick', labelsize = 25)
    plt.rc('ytick', labelsize = 35)
    plt.rc('axes', labelsize = 27)
    plt.rc('axes', titlesize = 30)
    plt.rc('legend', fontsize = 25)

    colors = ['lightblue', 'blue', 'lightgreen', 'green', 'pink', 'red']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (75, 10))
    median_color = dict(color = 'white')
    data_types = list()
    data_embeddings = list()

    data_types.append(ndcg_dict[str(votes)]['types']['32']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['128']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['30']['10'])
    data_types.append(ndcg_dict[str(votes)]['types_column']['32']['8'])
    data_types.append(ndcg_dict[str(votes)]['types_column']['128']['8'])
    data_types.append(ndcg_dict[str(votes)]['types_column']['30']['10'])
    data_types.append(ndcg_dict['baseline']['jaccard'])
    data_types.append(ndcg_dict['baseline']['bm25_entities'])
    data_types.append(ndcg_dict['baseline']['bm25_text'])

    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['30']['10'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['30']['10'])
    data_embeddings.append(ndcg_dict['baseline']['cosine'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_entities'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_text'])

    plot_types = ax1.boxplot(data_types, vert = True, patch_artist = True, medianprops = median_color, labels = ['T(V=32, BS=8)', 'T(V=128, BS=8)', 'T(V=30, BS=10)', 'TC(V=32, BS=8)', 'TC(V=128, BS=8)', 'TC(V=30, BS=10)', 'B - Jaccard', 'BM25 - entities', 'BM25 - text'])
    ax1.set_title('LSH Using Types - Top-' + str(k))

    plot_embeddings = ax2.boxplot(data_embeddings, vert = True, patch_artist = True, medianprops = median_color, labels = ['E(V=32, BS=8)', 'E(V=128, BS=8)', 'E(V=30, BS=10)', 'EC(V=32, BS=8)', 'EC(V=128, BS=8)', 'EC(V=30, BS=10)', 'B - cosine', 'BM25 - entities', 'BM25 - text'])
    ax2.set_title('LSH Using Embeddings - Top-' + str(k))

    for plot in (plot_types, plot_embeddings):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    ax1.set_xlabel('T = LSH of types, E = LSH of embeddings, TC = Column-based LSH of types, EC = Column-based LSH of embeddings, B = Brute-Force V = # permutation/projection vectors, BS = band size')

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')
        ax.vlines(6.5, 0, 1.0)

    plt.savefig(str(tuples) + '-tuple_plot_' + str(votes) + '_votes.pdf', format = 'pdf')
    plt.clf()

    # Plotting BM25 as a pre-filtering technique and comparing it to all configurations without aggregation

    plt.rc('xtick', labelsize = 25)
    plt.rc('ytick', labelsize = 35)
    plt.rc('axes', labelsize = 27)
    plt.rc('axes', titlesize = 30)
    plt.rc('legend', fontsize = 25)

    colors = ['lightblue', 'blue', 'lightgreen', 'brown']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (85, 10))
    median_color = dict(color = 'white')
    data_types = list()
    data_embeddings = list()

    data_types.append(ndcg_dict[str(votes)]['types']['32']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['128']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['30']['10'])
    data_types.append(ndcg_dict['baseline']['bm25_prefilter']['types'])
    data_types.append(ndcg_dict['baseline']['bm25_entities'])
    data_types.append(ndcg_dict['baseline']['bm25_text'])
    data_types.append(ndcg_dict['baseline']['jaccard'])

    data_embeddings.append(ndcg_dict[str(votes)]['types']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['types']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['types']['30']['10'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_prefilter']['embeddings'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_entities'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_text'])
    data_embeddings.append(ndcg_dict['baseline']['jaccard'])

    plot_types = ax1.boxplot(data_types, vert = True, patch_artist = True, medianprops = median_color, labels = ['T(V=32, BS=8)', 'T(V=128, BS=8)', 'T(V=30, BS=10)', 'BM25 pre-filtering - Jaccard', 'BM25 - entities', 'BM25 - text', 'B - Jaccard'])
    ax1.set_title('LSH Using Types - Top-' + str(k))

    plot_types = ax2.boxplot(data_types, vert = True, patch_artist = True, medianprops = median_color, labels = ['E(V=32, BS=8)', 'E(V=128, BS=8)', 'E(V=30, BS=10)', 'BM25 pre-filtering - cosine', 'BM25 - entities', 'BM25 - text', 'B - cosine'])
    ax2.set_title('LSH Using Embeddings - Top-' + str(k))

    for plot in (plot_types, plot_embeddings):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    ax1.set_xlabel('T = LSH of types, E = LSH of embeddings, B = Brute-Force V = # permutation/projection vectors, BS = band size')

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')
        ax.vlines(3.5, 0, 1.0)

    plt.savefig(str(tuples) + '-tuple_plot_' + str(votes) + '_votes_bm25_prefilter.pdf', format = 'pdf')
    plt.clf()

# Returns map: ['types'|'embeddings'|'baseline']->[<# BUCKETS: [150|300]>]->[<TOP-K: [10|100]>]->[NDCG SCORES]
def plot_ndcg(tuples):
    query_dir = 'queries/' + str(tuples) + '-tuple/'
    ground_truth_dir = '../../data/tables/SemanticTableSearchDataset/ground_truth/wikipedia_categories'
    corpus = '/data/tables/SemanticTableSearchDataset/table_corpus/tables'
    mapping_file = '../../data/tables/SemanticTableSearchDataset/table_corpus/wikipages_df.pickle'
    query_files = os.listdir(query_dir)
    table_files = full_corpus(corpus + '/../corpus')
    k = 10
    #votes = [1, 2, 3]
    votes = [1]
    ndcg = dict()

    for vote in votes:
        ndcg[str(vote)] = dict()
        ndcg[str(vote)]['types'] = dict()
        ndcg[str(vote)]['types_column'] = dict()
        ndcg[str(vote)]['embeddings'] = dict()
        ndcg[str(vote)]['embeddings_column'] = dict()
        ndcg[str(vote)]['types']['30'] = dict()
        ndcg[str(vote)]['types']['32'] = dict()
        ndcg[str(vote)]['types']['128'] = dict()
        ndcg[str(vote)]['types_column']['30'] = dict()
        ndcg[str(vote)]['types_column']['32'] = dict()
        ndcg[str(vote)]['types_column']['128'] = dict()
        ndcg[str(vote)]['embeddings']['30'] = dict()
        ndcg[str(vote)]['embeddings']['32'] = dict()
        ndcg[str(vote)]['embeddings']['128'] = dict()
        ndcg[str(vote)]['embeddings_column']['30'] = dict()
        ndcg[str(vote)]['embeddings_column']['32'] = dict()
        ndcg[str(vote)]['embeddings_column']['128'] = dict()
        ndcg[str(vote)]['types']['30']['10'] = list()
        ndcg[str(vote)]['types']['32']['8'] = list()
        ndcg[str(vote)]['types']['128']['8'] = list()
        ndcg[str(vote)]['types_column']['30']['10'] = list()
        ndcg[str(vote)]['types_column']['32']['8'] = list()
        ndcg[str(vote)]['types_column']['128']['8'] = list()
        ndcg[str(vote)]['embeddings']['30']['10'] = list()
        ndcg[str(vote)]['embeddings']['32']['8'] = list()
        ndcg[str(vote)]['embeddings']['128']['8'] = list()
        ndcg[str(vote)]['embeddings_column']['30']['10'] = list()
        ndcg[str(vote)]['embeddings_column']['32']['8'] = list()
        ndcg[str(vote)]['embeddings_column']['128']['8'] = list()
        ndcg['baseline'] = dict()
        ndcg['baseline']['jaccard'] = list()
        ndcg['baseline']['cosine'] = list()
        ndcg['baseline']['bm25_entities'] = list()
        ndcg['baseline']['bm25_text'] = list()
        ndcg['baseline']['bm25_prefilter'] = dict()
        ndcg['baseline']['bm25_prefilter']['types'] = list()
        ndcg['baseline']['bm25_prefilter']['embeddings'] = list()

        count = 0
        print('Vote = ' + str(vote))

        for query_file in query_files:
            count += 1

            query_id = query_file.split('.')[0]
            query_path = query_dir + query_file
            truth = ground_truth(query_path, ground_truth_dir, corpus, mapping_file)
            gt_rels = {table:0 for table in table_files}

            for relevance in truth[1]:
                gt_rels[relevance[1]] = relevance[0]

            # Types
            predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types']['30']['10'].append(ndcg_types)

            predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types']['32']['8'].append(ndcg_types)

            predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types']['128']['8'].append(ndcg_types)

            # Types - column aggregation
            predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types_column']['30']['10'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types_column']['32']['8'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['types_column']['128']['8'].append(ndcg_embeddings)

            # Embeddings
            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings']['30']['10'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings']['32']['8'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, tuples, k, gt_rels)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings']['128']['8'].append(ndcg_embeddings)

            # Embeddings - Column aggregation
            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings_column']['30']['10'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings_column']['32']['8'].append(ndcg_embeddings)

            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, tuples, k, gt_rels, False, True)

            if not predicted_relevance is None:
                ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg[str(vote)]['embeddings_column']['128']['8'].append(ndcg_embeddings)

            # Baseline
            predicted_relevance = predicted_scores(query_id, vote, 'jaccard', 32, 8, tuples, k, gt_rels, True, False)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['jaccard'].append(ndcg_baseline)

            predicted_relevance = predicted_scores(query_id, vote, 'cosine', 32, 8, tuples, k, gt_rels, True, False)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['cosine'].append(ndcg_baseline)

            predicted_relevance = predicted_scores(query_id, vote, 'entities', 32, 8, tuples, k, gt_rels, True, False, True)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['bm25_entities'].append(ndcg_baseline)

            predicted_relevance = predicted_scores(query_id, vote, 'text', 32, 8, tuples, k, gt_rels, True, False, True)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['bm25_text'].append(ndcg_baseline)

            predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuples, k, gt_rels, False, False, False, True)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['bm25_prefilter']['types'].append(ndcg_baseline)

            predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuples, k, gt_rels, False, False, False, True)

            if not predicted_relevance is None:
                ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                ndcg['baseline']['bm25_prefilter']['embeddings'].append(ndcg_baseline)

        gen_boxplots(ndcg, vote, tuples, k)

print('1-TUPLE QUERIES')
plot_ndcg(1)

print('5-TUPLE QUERIES')
plot_ndcg(5)
