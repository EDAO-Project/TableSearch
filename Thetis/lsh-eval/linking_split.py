import json
import os
import shutil

tables_dir = '/data/tables/SemanticTableSearchDataset/table_corpus/corpus/'
votes = 3
result_dir = 'results_linking/vote_' + str(votes) + '/all/'
coverages = None
k = 10

size1 = '40'
size2 = '60'
size3 = '80'

with open('/data/tables/linking_coverage/tables_coverage.json', 'r') as file:
    coverages = json.load(file)

# Types
queries_result_dir = result_dir + 'types/vectors_30/bandsize_10/1000/'
results_1_tuple = os.listdir(queries_result_dir + '1-tuple/search_output')
results_5_tuple = os.listdir(queries_result_dir + '5-tuple/search_output')

for query_result in results_1_tuple:
    filename = queries_result_dir + '1-tuple/search_output/' + query_result + '/filenameToScore.json'
    k_counter = 0

    # Create result file in all bins
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size1 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size2 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size3 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)

    # Copy to correct bins
    with open(filename, 'r') as file:
        result = json.load(file)
        out_dict_1 = {'scores': []}
        out_dict_2 = {'scores': []}
        out_dict_3 = {'scores': []}

        for table in result['scores']:
            if k_counter == k:
                break

            k_counter += 1
            table_id = table['tableID']
            coverage = coverages[table_id]

            if coverage < int(size1) / 100:
                out_dict_1['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size2) / 100:
                out_dict_2['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size3) / 100:
                out_dict_3['scores'].append({'tableID': table_id, 'score': table['score']})

        with open('results_linking/vote_' + str(votes) + '/' + size1 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_1, out)

        with open('results_linking/vote_' + str(votes) + '/' + size2 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_2, out)

        with open('results_linking/vote_' + str(votes) + '/' + size3 + '/types/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_3, out)

for query_result in results_5_tuple:
    filename = queries_result_dir + '5-tuple/search_output/' + query_result + '/filenameToScore.json'
    k_counter = 0

    # Create result file in all bins
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size1 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size2 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size3 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)

    # Copy to correct bins
    with open(filename, 'r') as file:
        result = json.load(file)
        out_dict_1 = {'scores': []}
        out_dict_2 = {'scores': []}
        out_dict_3 = {'scores': []}

        for table in result['scores']:
            if k_counter == k:
                break

            k_counter += 1
            table_id = table['tableID']
            coverage = coverages[table_id]

            if coverage < int(size1) / 100:
                out_dict_1['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size2) / 100:
                out_dict_2['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size3) / 100:
                out_dict_3['scores'].append({'tableID': table_id, 'score': table['score']})

        with open('results_linking/vote_' + str(votes) + '/' + size1 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_1, out)

        with open('results_linking/vote_' + str(votes) + '/' + size2 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_2, out)

        with open('results_linking/vote_' + str(votes) + '/' + size3 + '/types/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_3, out)

# Embeddings
queries_result_dir = result_dir + 'embeddings/vectors_30/bandsize_10/1000/'
results_1_tuple = os.listdir(queries_result_dir + '1-tuple/search_output')
results_5_tuple = os.listdir(queries_result_dir + '5-tuple/search_output')

for query_result in results_1_tuple:
    filename = queries_result_dir + '1-tuple/search_output/' + query_result + '/filenameToScore.json'
    k_counter = 0

    # Create result file in all bins
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size1 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size2 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size3 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result, exist_ok = True)

    # Copy to correct bins
    with open(filename, 'r') as file:
        result = json.load(file)
        out_dict_1 = {'scores': []}
        out_dict_2 = {'scores': []}
        out_dict_3 = {'scores': []}

        for table in result['scores']:
            if k_counter == k:
                break

            k_counter += 1
            table_id = table['tableID']
            coverage = coverages[table_id]

            if coverage < int(size1) / 100:
                out_dict_1['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size2) / 100:
                out_dict_2['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size3) / 100:
                out_dict_3['scores'].append({'tableID': table_id, 'score': table['score']})

        with open('results_linking/vote_' + str(votes) + '/' + size1 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_1, out)

        with open('results_linking/vote_' + str(votes) + '/' + size2 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_2, out)

        with open('results_linking/vote_' + str(votes) + '/' + size3 + '/embeddings/vectors_30/bandsize_10/10/1-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_3, out)

for query_result in results_5_tuple:
    filename = queries_result_dir + '5-tuple/search_output/' + query_result + '/filenameToScore.json'
    k_counter = 0

    # Create result file in all bins
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size1 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size2 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)
    os.makedirs('results_linking/vote_' + str(votes) + '/' + size3 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result, exist_ok = True)

    # Copy to correct bins
    with open(filename, 'r') as file:
        result = json.load(file)
        out_dict_1 = {'scores': []}
        out_dict_2 = {'scores': []}
        out_dict_3 = {'scores': []}

        for table in result['scores']:
            if k_counter == k:
                break

            k_counter += 1
            table_id = table['tableID']
            coverage = coverages[table_id]

            if coverage < int(size1) / 100:
                out_dict_1['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size2) / 100:
                out_dict_2['scores'].append({'tableID': table_id, 'score': table['score']})

            if coverage < int(size3) / 100:
                out_dict_3['scores'].append({'tableID': table_id, 'score': table['score']})

        with open('results_linking/vote_' + str(votes) + '/' + size1 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_1, out)

        with open('results_linking/vote_' + str(votes) + '/' + size2 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_2, out)

        with open('results_linking/vote_' + str(votes) + '/' + size3 + '/embeddings/vectors_30/bandsize_10/10/5-tuple/search_output/' + query_result + '/filenameToScore.json', 'w') as out:
            json.dump(out_dict_3, out)
