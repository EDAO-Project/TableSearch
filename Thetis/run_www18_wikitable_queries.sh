#!/bin/bash
'''
Run the type-similarity baseline algorithm for every query over the www18_wikitables dataset
'''

# Run the search pipeline for each query in the specified directory
queries_dir="/data/queries/www18_wikitables/queries/*.json"
tables_dir="/data/tables/wikitables/files/wikitables_per_query/"
output_dir="/data/search/www18_wikitables/"

# use_embeddings=false
# single_column_per_query_entity=true

# The set of embedding similarity functions
embeddingSimFunctions=( "ang_cos" "norm_cos" "abs_cos" )

for filepath in $queries_dir; do

    # Get filename and remove the file extension
    filename="${filepath##*/}"
    filename="${filename%.*}"

    # Create the Index output_dir
    index_out_dir=/data/index/wikitables_per_query/$filename/
    mkdir -p $index_out_dir

    ###############----- Run Indexing Step -----###############
    java -jar target/Thetis.0.1.jar  index --table-type wikitables \
    --table-dir  $tables_dir/$filename/ --output-dir $index_out_dir


    # ###############----- Run Search Step -----###############

    # Baseline (Single Column Per Query Entity)
    java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_out_dir \
    --query-file $filepath --table-dir $tables_dir/$filename/ \
    --output-dir $output_dir/baseline/single_column_per_entity/$filename/ --singleColumnPerQueryEntity

    # Pre-trained Embeddings (Single Column Per Query Entity)
    for embSimFunc in "${embeddingSimFunctions[@]}"; do
        java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_out_dir \
        --query-file $filepath --table-dir $tables_dir/$filename/ \
        --output-dir $output_dir/embeddings/$embSimFunc/single_column_per_entity/$filename/ \
        --singleColumnPerQueryEntity --usePretrainedEmbeddings --embeddingSimilarityFunction $embSimFunc
    done


done