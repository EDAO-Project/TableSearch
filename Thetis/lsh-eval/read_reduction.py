import json
import os

vote = 3
tuples = 5

base = 'results/vote_' + str(vote) + '/embeddings/'
#base = 'results/vote_' + str(vote) + '/aggregation/types/'
vectors = os.listdir(base)

for vector in vectors:
    filename = base + vector + '/'
    band_sizes = os.listdir(filename)

    for band_size in band_sizes:
        filename_full = filename + band_size + '/100/' + str(tuples) + '-tuple/search_output/'
        outputs = os.listdir(filename_full)
        print('\n' + vector + ' - ' + band_size)

        for output in outputs:
            out_filename = filename_full + output + '/filenameToScore.json'

            with open(out_filename) as file:
                j = json.load(file)
                print(str(float(j['reduction']) * 100).replace('.', ','))
