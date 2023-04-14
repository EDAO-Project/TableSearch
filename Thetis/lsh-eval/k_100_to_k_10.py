# results/vote_3/types/vectors_128/bandsize_8/100/1-tuple/search_output

import os
import json

def convert_to_k(k, path):
    query_dirs = os.listdir(path)
    new_dir = path[0:len(path) - len(path.split('/')[-1])] + str(k)
    os.mkdir(new_dir)

    for query_dir in query_dirs:
        query_result_folder_path = path + '/' + query_dir + '/search_output'

        os.mkdir(new_dir + '/' + query_dir)
        os.mkdir(new_dir + '/' + query_dir + '/search_output')

        query_folders = os.listdir(query_result_folder_path)

        for query_folder in query_folders:
            with open(query_result_folder_path + '/' + query_folder + '/filenameToScore.json', 'r') as json_file:
                object = json.load(json_file)
                os.mkdir(new_dir + '/' + query_dir + '/search_output/' + query_folder)

                with open(new_dir + '/' + query_dir + '/search_output/' + query_folder + '/filenameToScore.json', 'w') as output_file:
                    i = 0
                    results_k_10 = dict()
                    results_k_10['scores'] = list()

                    for result in object['scores']:
                        i += 1
                        results_k_10['scores'].append(result)

                        if (i == k):
                            break

                    results_k_10['runtime'] = object['runtime']
                    results_k_10['threads'] = object['threads']
                    results_k_10['algorithm'] = object['algorithm']
                    output_file.write(json.dumps(results_k_10, indent = 4))

convert_to_k(10, 'results/vote_1/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_1/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_1/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_1/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_1/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_1/embeddings/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_1/aggregation/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_1/aggregation/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_1/aggregation/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_1/aggregation/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_1/aggregation/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_1/aggregation/embeddings/vectors_30/bandsize_10/100')

convert_to_k(10, 'results/vote_2/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_2/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_2/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_2/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_2/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_2/embeddings/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_2/aggregation/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_2/aggregation/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_2/aggregation/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_2/aggregation/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_2/aggregation/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_2/aggregation/embeddings/vectors_30/bandsize_10/100')

convert_to_k(10, 'results/vote_3/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_3/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_3/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_3/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_3/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_3/embeddings/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_3/aggregation/types/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_3/aggregation/types/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_3/aggregation/types/vectors_30/bandsize_10/100')
convert_to_k(10, 'results/vote_3/aggregation/embeddings/vectors_128/bandsize_8/100')
convert_to_k(10, 'results/vote_3/aggregation/embeddings/vectors_32/bandsize_8/100')
convert_to_k(10, 'results/vote_3/aggregation/embeddings/vectors_30/bandsize_10/100')

convert_to_k(10, 'results/baseline/baseline_jaccard/100')
convert_to_k(10, 'results/baseline/baseline_cosine/100')
convert_to_k(10, 'results/baseline/baseline_bm25_prefiltering/types/100')
convert_to_k(10, 'results/baseline/baseline_bm25_prefiltering/embeddings/100')
