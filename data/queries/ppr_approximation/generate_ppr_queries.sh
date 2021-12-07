#!/bin/bash

q_size=20
num_runs=5

# Loop over each seed
for seed in `seq 1 $num_runs`
do

    python generate_random_queries.py --query_size $q_size \
    --output_dir queries/qsize_$q_size/seed_$seed/ \
    --seed $seed --entities_path ../../index/www18_wikitables/entityToIDF.json

done