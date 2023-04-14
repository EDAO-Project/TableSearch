import json
import os

vote = 3
tuples = 1

base = 'results_new/vote_' + str(vote) + '/types/'
#base = 'results/vote_' + str(vote) + '/aggregation/types/'
vectors = os.listdir(base)

for vector in vectors:
    filename = base + vector + '/'
    band_sizes = os.listdir(filename)

    for band_size in band_sizes:
        filename_full = filename + band_size + '/10/' + str(tuples) + '-tuple/search_output/'
        outputs = os.listdir(filename_full)
        print('\n' + vector + ' - ' + band_size)

        for output in outputs:
            out_filename = filename_full + output + '/filenameToScore.json'
            sum = 0

            with open(out_filename) as file:
                j = json.load(file)
                sum += float(j["runtime"]) / 1000000000
                print(str(sum).replace('.', ','))

            """with open(out_filename.replace('runtime_1', 'runtime_2')) as file:
                j = json.load(file)
                sum += float(j["runtime"]) / 1000000000

            with open(out_filename.replace('runtime_1', 'runtime_3')) as file:
                j = json.load(file)
                sum += float(j["runtime"]) / 1000000000

            print(str(float(sum) / 3).replace('.', ','))"""
