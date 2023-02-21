import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
import json
import os
import pickle
import random
import itertools
import operator
import statistics

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
def predicted_scores(query_id, votes, mode, vectors, band_size, tuples, k, gt_tables, is_baseline = False, is_column_aggregation = False, is_bm25 = False, is_bm25_prefilter = False, get_only_tables = False, top = None):
    path = 'results/vote_' + str(votes) + '/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    if (is_baseline):
        path = 'results/baseline/baseline_' + mode + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    elif (is_column_aggregation):
        path = 'results/vote_' + str(votes) + '/aggregation/' + mode + '/vectors_' + str(vectors) + '/bandsize_' + str(band_size) + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

    if (is_bm25):
        path = 'results/baseline/bm25/' + str(k) + '/' + str(tuples) + '-tuple/' + mode + '/content.txt'

    elif (is_bm25_prefilter):
        path = 'results/baseline/baseline_bm25_prefiltering/' + mode + '/' + str(k) + '/' + str(tuples) + '-tuple/search_output/' + query_id + '/filenameToScore.json'

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

        sort = list(sorted(scores.items(), key = operator.itemgetter(1), reverse = True))

        if (not top is None):
            sort = sort[:int(top)]

        if (get_only_tables):
            return [e[0] for e in sort]

        return list(scores.values())

def full_corpus(base_dir):
    files = os.listdir(base_dir)
    tables = list()

    for file in files:
        tables.append(file)

    return tables


# Boxplot of precision/recall
def gen_quality_boxplot(precision_1_tuple, recall_1_tuple, precision_5_tuple, recall_5_tuple, votes, k, mix_1_tuple = None, mix_5_tuple = None):
    plt.rc('xtick', labelsize = 10)
    plt.rc('ytick', labelsize = 10)
    plt.rc('axes', labelsize = 15)
    plt.rc('axes', titlesize = 20)
    plt.rc('legend', fontsize = 25)

    colors = ['lightgreen', 'darkgreen', 'blue', 'lightblue', 'pink', 'red']
    fig, axs = plt.subplots(nrows = 2, ncols = 2, figsize = (20, 10))
    precision1 = list()
    recall1 = list()
    precision5 = list()
    recall5 = list()

    precision1.append(precision_1_tuple['T(30, 10)'])
    precision1.append(precision_1_tuple['E(30, 10)'])
    precision1.append(precision_1_tuple['BT - Types'])
    precision1.append(precision_1_tuple['BT - Embeddings'])
    #precision1.append(precision_1_tuple['BM25 - Entity'])
    precision1.append(precision_1_tuple['BM25 - Text'])

    recall1.append(recall_1_tuple['T(30, 10)'])
    recall1.append(recall_1_tuple['E(30, 10)'])
    recall1.append(recall_1_tuple['BT - Types'])
    recall1.append(recall_1_tuple['BT - Embeddings'])
    #recall1.append(recall_1_tuple['BM25 - Entity'])
    recall1.append(recall_1_tuple['BM25 - Text'])

    precision5.append(precision_5_tuple['T(30, 10)'])
    precision5.append(precision_5_tuple['E(30, 10)'])
    precision5.append(precision_5_tuple['BT - Types'])
    precision5.append(precision_5_tuple['BT - Embeddings'])
    #precision5.append(precision_5_tuple['BM25 - Entity'])
    precision5.append(precision_5_tuple['BM25 - Text'])

    recall5.append(recall_5_tuple['T(30, 10)'])
    recall5.append(recall_5_tuple['E(30, 10)'])
    recall5.append(recall_5_tuple['BT - Types'])
    recall5.append(recall_5_tuple['BT - Embeddings'])
    #recall5.append(recall_5_tuple['BM25 - Entity'])
    recall5.append(recall_5_tuple['BM25 - Text'])

    plot_precision1 = axs[0, 0].boxplot(precision1, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['T(30, 10)', 'E(30, 10)', 'BFT', 'BFC', 'BM25T'])
    axs[0, 0].set_title('Precision@' + str(k) + ' (1-tuple queries)')
    axs[0, 0].yaxis.grid(True)
    axs[0, 0].set_ylabel('Fraction')
    axs[0, 0].vlines(4.5, 0, 1.0)

    plot_recall1 = axs[0, 1].boxplot(recall1, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['T(30, 10)', 'E(30, 10)', 'BFT', 'BFC', 'BM25T'])
    axs[0, 1].set_title('Recall@' + str(k) + ' (1-tuple queries)')
    axs[0, 1].yaxis.grid(True)
    axs[0, 1].set_ylabel('Fraction')
    axs[0, 1].vlines(4.5, 0, 1.0)

    plot_precision5 = axs[1, 0].boxplot(precision5, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['T(30, 10)', 'E(30, 10)', 'BFT', 'BFC', 'BM25T'])
    axs[1, 0].set_title('Precision@' + str(k) + ' (5-tuple queries)')
    axs[1, 0].yaxis.grid(True)
    axs[1, 0].set_ylabel('Fraction')
    axs[1, 0].vlines(4.5, 0, 1.0)

    plot_recall5 = axs[1, 1].boxplot(recall5, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['T(30, 10)', 'E(30, 10)', 'BFT', 'BFC', 'BM25T'])
    axs[1, 1].set_title('Recall@' + str(k) + ' (5-tuple queries)')
    axs[1, 1].yaxis.grid(True)
    axs[1, 1].set_ylabel('Fraction')
    axs[1, 1].vlines(4.5, 0, 1.0)

    for plot in (plot_precision1, plot_recall1, plot_precision5, plot_recall5):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    plt.savefig('Precision-Recall_' + str(votes) + '-votes.pdf', format = 'pdf')
    plt.clf()

    print('1-tuple')
    print('T(30, 10): ' + str(statistics.median(recall1[0])))
    print('E(30, 10): ' + str(statistics.median(recall1[1])))
    print('BFJ: ' + str(statistics.median(recall1[2])))
    print('BFC: ' + str(statistics.median(recall1[3])))
    print('BM25E: ' + str(statistics.median(recall1[4])))
    print('BM25T: ' + str(statistics.median(recall1[5])))
    print('\n5-tuple')
    print('T(30, 10): ' + str(statistics.median(recall5[0])))
    print('E(30, 10): ' + str(statistics.median(recall5[1])))
    print('BFJ: ' + str(statistics.median(recall5[2])))
    print('BFC: ' + str(statistics.median(recall5[3])))
    print('BM25E: ' + str(statistics.median(recall5[4])))
    print('BM25T: ' + str(statistics.median(recall5[5])))
    print('\nMixing\n1-tuple')

    # Mixing Thetis and BM25 - recall only
    if (mix_1_tuple is None or mix_5_tuple is None):
        pass

    plt.rc('xtick', labelsize = 10)
    plt.rc('ytick', labelsize = 10)
    plt.rc('axes', labelsize = 15)
    plt.rc('axes', titlesize = 18)
    plt.rc('legend', fontsize = 25)

    colors = ['blue', 'red']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (12, 4))
    recall1 = list()
    recall5 = list()

    recall1.append(mix_1_tuple['BT - Types - Mix'])
    recall1.append(mix_1_tuple['BT - Embeddings - Mix'])
    recall5.append(mix_5_tuple['BT - Types - Mix'])
    recall5.append(mix_5_tuple['BT - Embeddings - Mix'])

    plot_1_tuple = ax1.boxplot(recall1, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['BFTC', 'BFCC'])
    ax1.set_title('Recall@' + str(k) + ' (1-Tuple Queries)')
    ax1.yaxis.grid(True)
    ax1.set_ylabel('Fraction')

    plot_5_tuple = ax2.boxplot(recall5, vert = True, patch_artist = True, medianprops = dict(color = 'white'), labels = ['BFTC', 'BFTCC'])
    ax2.set_title('Recall@' + str(k) + ' (5-Tuple Queries)')
    ax2.yaxis.grid(True)
    ax2.set_ylabel('Fraction')

    for plot in (plot_1_tuple, plot_5_tuple):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    plt.savefig('mixed-recall_' + str(votes) + '-votes.pdf', format = 'pdf')
    plt.clf()

    print('BFJ: ' + statistics.median(recall1[0]))
    print('BFC: ' + statistics.median(recall1[0]))
    print('5-tuple')
    print('BFJ: ' + statistics.median(recall5[0]))
    print('BFC: ' + statistics.median(recall5[0]))

# Mainly NDCG plots
def gen_boxplots(ndcg_dict, votes, tuples, k):
    # First the plots of all configurations

    plt.rc('xtick', labelsize = 17)
    plt.rc('ytick', labelsize = 20)
    plt.rc('axes', labelsize = 16)
    plt.rc('axes', titlesize = 20)
    plt.rc('legend', fontsize = 20)

    colors = ['lightblue', 'blue', 'lightgreen', 'green', 'pink', 'red']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (28, 4))
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
    #data_types.append(ndcg_dict['baseline']['bm25_entities'])
    data_types.append(ndcg_dict['baseline']['bm25_text'])

    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['30']['10'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings_column']['30']['10'])
    data_embeddings.append(ndcg_dict['baseline']['cosine'])
    #data_embeddings.append(ndcg_dict['baseline']['bm25_entities'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_text'])

    plot_types = ax1.boxplot(data_types, vert = True, patch_artist = True, medianprops = median_color, labels = ['(32, 8)', '(128, 8)', '(30, 10)', '(32, 8)*', '(128, 8)*', '(30, 10)*', 'BFJ', 'BM25T'])
    ax1.set_title('LSH Using Types - Top-' + str(k))

    plot_embeddings = ax2.boxplot(data_embeddings, vert = True, patch_artist = True, medianprops = median_color, labels = ['(32, 8)', '(128, 8)', '(30, 10)', '(32, 8)*', '(128, 8)*', '(30, 10)*', 'BFC', 'BM25T'])
    ax2.set_title('LSH Using Embeddings - Top-' + str(k))

    for plot in (plot_types, plot_embeddings):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')
        ax.vlines(6.5, 0, 1.0)

    plt.savefig(str(tuples) + '-tuple_plot_' + str(votes) + '_votes.pdf', format = 'pdf')
    plt.clf()

    # Plotting BM25 as a pre-filtering technique and comparing it to all configurations without aggregation

    plt.rc('xtick', labelsize = 16)
    plt.rc('ytick', labelsize = 20)
    plt.rc('axes', labelsize = 16)
    plt.rc('axes', titlesize = 22)
    plt.rc('legend', fontsize = 20)

    colors = ['lightblue', 'blue', 'lightgreen', 'brown']
    fig, (ax1, ax2) = plt.subplots(nrows = 1, ncols = 2, figsize = (20, 4))
    median_color = dict(color = 'white')
    data_types = list()
    data_embeddings = list()

    data_types.append(ndcg_dict[str(votes)]['types']['32']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['128']['8'])
    data_types.append(ndcg_dict[str(votes)]['types']['30']['10'])
    data_types.append(ndcg_dict['baseline']['bm25_prefilter']['types'])
    #data_types.append(ndcg_dict['baseline']['bm25_entities'])
    data_types.append(ndcg_dict['baseline']['bm25_text'])
    data_types.append(ndcg_dict['baseline']['jaccard'])

    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['32']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['128']['8'])
    data_embeddings.append(ndcg_dict[str(votes)]['embeddings']['30']['10'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_prefilter']['embeddings'])
    #data_embeddings.append(ndcg_dict['baseline']['bm25_entities'])
    data_embeddings.append(ndcg_dict['baseline']['bm25_text'])
    data_embeddings.append(ndcg_dict['baseline']['cosine'])

    plot_types = ax1.boxplot(data_types, vert = True, patch_artist = True, medianprops = median_color, labels = ['(32, 8)', '(128, 8)', '(30, 10)', 'BM25JP', 'BM25T', 'BFJ'])
    ax1.set_title('LSH Using Types - Top-' + str(k))

    plot_embeddings = ax2.boxplot(data_embeddings, vert = True, patch_artist = True, medianprops = median_color, labels = ['(32, 8)', '(128, 8)', '(30, 10)', 'BM25PC', 'BM25T', 'BFC'])
    ax2.set_title('LSH Using Embeddings - Top-' + str(k))

    for plot in (plot_types, plot_embeddings):
        for patch, color in zip(plot['boxes'], colors):
            patch.set_facecolor(color)

    for ax in [ax1, ax2]:
        ax.yaxis.grid(True)
        ax.set_ylabel('NDCG')
        ax.vlines(3.5, 0, 1.0)

    plt.savefig(str(tuples) + '-tuple_plot_' + str(votes) + '_votes_bm25_prefilter.pdf', format = 'pdf')
    plt.clf()

# Computes precision
def precision(tables, gt):
    count = 0

    for table in tables:
        if table in gt:
            count += 1

    if len(tables) == 0:
        return 0

    return float(count) / len(tables)

# Computes recall
def recall(tables, gt):
    count = 0

    for table in tables:
        if table in gt:
            count += 1

    if len(tables) == 0:
        return 0

    return float(count) / len(gt)

def sort_truth_val(elem):
    return elem[0]

# Returns map: ['types'|'embeddings'|'baseline']->[<# BUCKETS: [150|300]>]->[<TOP-K: [10|100]>]->[NDCG SCORES]
def plot_ndcg():
    k = 100
    votes = [3]
    tuples = [1, 5]
    ndcg = dict()
    precision_dict = dict()
    recall_dict = dict()
    recall_mix_dict = dict()
    precision_dict['1'] = dict()
    recall_dict['1'] = dict()
    precision_dict['5'] = dict()
    recall_dict['5'] = dict()
    recall_mix_dict['1'] = dict()
    recall_mix_dict['5'] = dict()
    precision_dict['1']['T(30, 10)'] = list()
    precision_dict['1']['E(30, 10)'] = list()
    precision_dict['1']['BT - Types'] = list()
    precision_dict['1']['BT - Embeddings'] = list()
    precision_dict['1']['BM25 - Entity'] = list()
    precision_dict['1']['BM25 - Text'] = list()
    recall_dict['1']['T(30, 10)'] = list()
    recall_dict['1']['E(30, 10)'] = list()
    recall_dict['1']['BT - Types'] = list()
    recall_dict['1']['BT - Embeddings'] = list()
    recall_dict['1']['BM25 - Entity'] = list()
    recall_dict['1']['BM25 - Text'] = list()
    precision_dict['5']['T(30, 10)'] = list()
    precision_dict['5']['E(30, 10)'] = list()
    precision_dict['5']['BT - Types'] = list()
    precision_dict['5']['BT - Embeddings'] = list()
    precision_dict['5']['BM25 - Entity'] = list()
    precision_dict['5']['BM25 - Text'] = list()
    recall_dict['5']['T(30, 10)'] = list()
    recall_dict['5']['E(30, 10)'] = list()
    recall_dict['5']['BT - Types'] = list()
    recall_dict['5']['BT - Embeddings'] = list()
    recall_dict['5']['BM25 - Entity'] = list()
    recall_dict['5']['BM25 - Text'] = list()
    recall_mix_dict['1']['BT - Types - Mix'] = list()
    recall_mix_dict['5']['BT - Types - Mix'] = list()
    recall_mix_dict['1']['BT - Embeddings - Mix'] = list()
    recall_mix_dict['5']['BT - Embeddings - Mix'] = list()

    for tuple in tuples:
        query_dir = 'queries/' + str(tuple) + '-tuple/'
        ground_truth_dir = '../../data/tables/SemanticTableSearchDataset/ground_truth/wikipedia_categories'
        corpus = '/data/tables/SemanticTableSearchDataset/table_corpus/tables'
        mapping_file = '../../data/tables/SemanticTableSearchDataset/table_corpus/wikipages_df.pickle'
        query_files = os.listdir(query_dir)
        table_files = full_corpus(corpus + '/../corpus')
        print(str(tuple) + '-TUPLE QUERIES')

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
                truth_tables = truth[1]
                truth_tables.sort(reverse = True, key = sort_truth_val)
                top_k_truth_tables = list()
                count_k = 0
                gt_rels = {table:0 for table in table_files}

                for relevance in truth[1]:
                    gt_rels[relevance[1]] = relevance[0]

                for truth_table in truth_tables:
                    if count_k == k:
                        break

                    top_k_truth_tables.append(truth_table[1])

                # Types
                predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, tuple, k, gt_rels)
                predicted_tables = predicted_scores(query_id, vote, 'types', 30, 10, tuple, k, gt_rels, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types']['30']['10'].append(ndcg_types)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['T(30, 10)'].append(precision_val)
                    recall_dict[str(tuple)]['T(30, 10)'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuple, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types']['32']['8'].append(ndcg_types)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, tuple, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_types = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types']['128']['8'].append(ndcg_types)

                # Types - column aggregation
                predicted_relevance = predicted_scores(query_id, vote, 'types', 30, 10, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types_column']['30']['10'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types_column']['32']['8'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 128, 8, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['types_column']['128']['8'].append(ndcg_embeddings)

                # Embeddings
                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, tuple, k, gt_rels)
                predicted_tables = predicted_scores(query_id, vote, 'embeddings', 30, 10, tuple, k, gt_rels, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings']['30']['10'].append(ndcg_embeddings)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['E(30, 10)'].append(precision_val)
                    recall_dict[str(tuple)]['E(30, 10)'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuple, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings']['32']['8'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, tuple, k, gt_rels)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings']['128']['8'].append(ndcg_embeddings)

                # Embeddings - Column aggregation
                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 30, 10, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings_column']['30']['10'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings_column']['32']['8'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 128, 8, tuple, k, gt_rels, False, True)

                if not predicted_relevance is None:
                    ndcg_embeddings = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg[str(vote)]['embeddings_column']['128']['8'].append(ndcg_embeddings)

                predicted_relevance = predicted_scores(query_id, vote, 'jaccard', 32, 8, tuple, k, gt_rels, True, False)
                predicted_tables = predicted_scores(query_id, vote, 'jaccard', 32, 8, tuple, k, gt_rels, True, False, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['jaccard'].append(ndcg_baseline)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['BT - Types'].append(precision_val)
                    recall_dict[str(tuple)]['BT - Types'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'cosine', 32, 8, tuple, k, gt_rels, True, False)
                predicted_tables = predicted_scores(query_id, vote, 'cosine', 32, 8, tuple, k, gt_rels, True, False, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['cosine'].append(ndcg_baseline)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['BT - Embeddings'].append(precision_val)
                    recall_dict[str(tuple)]['BT - Embeddings'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'entities', 32, 8, tuple, k, gt_rels, True, False, True)
                predicted_tables = predicted_scores(query_id, vote, 'entities', 32, 8, tuple, k, gt_rels, True, False, True, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['bm25_entities'].append(ndcg_baseline)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['BM25 - Entity'].append(precision_val)
                    recall_dict[str(tuple)]['BM25 - Entity'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'text', 32, 8, tuple, k, gt_rels, True, False, True)
                predicted_tables = predicted_scores(query_id, vote, 'text', 32, 8, tuple, k, gt_rels, True, False, True, get_only_tables = True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['bm25_text'].append(ndcg_baseline)

                if vote == 3 and not predicted_tables is None:
                    precision_val = precision(predicted_tables, top_k_truth_tables)
                    recall_val = recall(predicted_tables, top_k_truth_tables)
                    precision_dict[str(tuple)]['BM25 - Text'].append(precision_val)
                    recall_dict[str(tuple)]['BM25 - Text'].append(recall_val)

                predicted_relevance = predicted_scores(query_id, vote, 'types', 32, 8, tuple, k, gt_rels, False, False, False, True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['bm25_prefilter']['types'].append(ndcg_baseline)

                predicted_relevance = predicted_scores(query_id, vote, 'embeddings', 32, 8, tuple, k, gt_rels, False, False, False, True)

                if not predicted_relevance is None:
                    ndcg_baseline = ndcg_score(np.array([list(gt_rels.values())]), np.array([predicted_relevance]), k = k)
                    ndcg['baseline']['bm25_prefilter']['embeddings'].append(ndcg_baseline)

                predicted_tables_types = predicted_scores(query_id, vote, 'jaccard', 32, 8, tuple, k, gt_rels, True, False, get_only_tables = True, top = k / 2)
                predicted_tables_bm25 = predicted_scores(query_id, vote, 'text', 32, 8, tuple, k, gt_rels, True, False, True, get_only_tables = True, top = k / 2)

                if not predicted_tables_types is None and not predicted_tables_bm25 is None:
                    mix = set(predicted_tables_types).union(set(predicted_tables_bm25))
                    recall_val = recall(mix, top_k_truth_tables)
                    recall_mix_dict[str(tuple)]['BT - Types - Mix'].append(recall_val)

                predicted_tables_embeddings = predicted_scores(query_id, vote, 'cosine', 32, 8, tuple, k, gt_rels, True, False, get_only_tables = True, top = k / 2)

                if not predicted_tables_embeddings is None and not predicted_tables_bm25 is None:
                    mix = set(predicted_tables_embeddings).union(set(predicted_tables_bm25))
                    recall_val = recall(mix, top_k_truth_tables)
                    recall_mix_dict[str(tuple)]['BT - Embeddings - Mix'].append(recall_val)

            #gen_boxplots(ndcg, vote, tuple, k)

        gen_quality_boxplot(precision_dict['1'], recall_dict['1'], precision_dict['5'], precision_dict['5'], votes, k, mix_1_tuple = recall_mix_dict['1'], mix_5_tuple = recall_mix_dict['5'])

plot_ndcg()
