#!/bin/bash

# General Directory Parameters
# output_dir_base="/data/search/expanded_wikipages/filtered_queries/"
# queries_dir_base="/data/queries/wikipages/queries/expanded_wikipages/filtered_queries/"
# tables_dir="/data/tables/wikipages/wikipages_expanded_dataset/tables/"
# index_dir="/data/index/wikipages_expanded/"

#####----- Testing Dataset Parameters -----##### 
output_dir_base="/data/search/wikipages_test/filtered_queries/"
queries_dir_base="/data/queries/wikipages/queries/wikipages_test_dataset/filtered_queries/"
tables_dir="/data/tables/wikipages/wikipages_test_dataset/tables/"
index_dir="/data/index/wikipages_test_dataset/"

min_tuple_width=2
search_mode="embeddings"

# Brute Force Embeddings Parameters
embedding_similarity_function="norm_cos"         # Must be one of: [norm_cos, abs_cos, ang_cos]
embeddings_file_path="/data/embeddings/embeddings_expanded.json"

# PPR Parameters
ppr_index_dir="/data/index/wikitables/"
min_threshold=0.01
num_particles=1000
topK=400

for num_tuples_per_query in {1,2,5,10}; do
    queries_dir=${queries_dir_base}minTupleWidth_${min_tuple_width}_tuplesPerQuery_${num_tuples_per_query}/*
    output_dir=${output_dir_base}minTupleWidth_${min_tuple_width}_tuplesPerQuery_${num_tuples_per_query}/
    mkdir -p $output_dir 

    # Loop over query in the $queries_dir
    for query_path in $queries_dir; do
        # Get the wikipage from the path
        wikipage="$(basename -- $query_path)"
        wikipage="${wikipage%.*}"

        search_mode="adjusted_jaccard"
        # # Run Adjusted Jaccard (Unweighted + avg_similarity_per_col)\
        # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${search_mode}/unweighted/avg_similarity_per_col/$wikipage/ \
        # --singleColumnPerQueryEntity --adjustedJaccardSimilarity

        # # Run Adjusted Jaccard (Unweighted + max_similarity_per_col)\
        # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${search_mode}/unweighted/max_similarity_per_col/$wikipage/ \
        # --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn

        # # Run Adjusted Jaccard (Unweighted + max_similarity_per_col) with hungarianAlgorithmSameAlignmentAcrossTuples
        # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${search_mode}/unweighted/max_similarity_per_col_same_alignment/$wikipage/ \
        # --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn --hungarianAlgorithmSameAlignmentAcrossTuples

        # # Run Adjusted Jaccard (Weighted + avg_similarity_per_col)
        # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${search_mode}/weighted/avg_similarity_per_col/$wikipage/ \
        # --singleColumnPerQueryEntity --adjustedJaccardSimilarity --weightedJaccardSimilarity

        # # Run Adjusted Jaccard (Weighted + max_similarity_per_col)
        # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${search_mode}/weighted/max_similarity_per_col/$wikipage/ \
        # --singleColumnPerQueryEntity --adjustedJaccardSimilarity --weightedJaccardSimilarity --useMaxSimilarityPerColumn

        ############## EMBEDDINGS ##############
        search_mode="embeddings"
       
        # Run pipeline using each specified embedding similarity function
        embedding_sim_functions='abs_cos norm_cos ang_cos'
        for embedding_similarity_function in $embedding_sim_functions; do
            echo $embedding_similarity_function

            # # Run Brute Force Approach using Embeddings (Single Column Per Query Entity + avg_similarity_per_col)
            # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
            # --query-file $query_path --table-dir $tables_dir \
            # --output-dir $output_dir/${search_mode}/$embedding_similarity_function/avg_similarity_per_col/$wikipage/ --singleColumnPerQueryEntity --adjustedJaccardSimilarity \
            # --usePretrainedEmbeddings --embeddingsInputMode file  --embeddingsPath  $embeddings_file_path --embeddingSimilarityFunction $embedding_similarity_function

            # # Run Brute Force Approach using Embeddings (Single Column Per Query Entity + max_similarity_per_col) 
            # java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
            # --query-file $query_path --table-dir $tables_dir \
            # --output-dir $output_dir/${search_mode}/$embedding_similarity_function/max_similarity_per_col/$wikipage/ --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn \
            # --usePretrainedEmbeddings --embeddingsInputMode file  --embeddingsPath  $embeddings_file_path --embeddingSimilarityFunction $embedding_similarity_function

            # Run Brute Force Approach using Embeddings Using Database To Query (Single Column Per Query Entity + avg_similarity_per_col)
            java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
            --query-file $query_path --table-dir $tables_dir \
            --output-dir $output_dir/${search_mode}/$embedding_similarity_function/avg_similarity_per_col/$wikipage/ 
            --singleColumnPerQueryEntity --adjustedJaccardSimilarity \
            --usePretrainedEmbeddings --embeddingsInputMode database --config ./config.properties --embeddingSimilarityFunction $embedding_similarity_function  

            # Run Brute Force Approach using Embeddings Using Database To Query (Single Column Per Query Entity + max_similarity_per_col)
            java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir $index_dir \
            --query-file $query_path --table-dir $tables_dir \
            --output-dir $output_dir/${search_mode}/$embedding_similarity_function/max_similarity_per_col/$wikipage/ 
            --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn \
            --usePretrainedEmbeddings --embeddingsInputMode database --config ./config.properties --embeddingSimilarityFunction $embedding_similarity_function
        done

        ############## PPR ##############

        # # Run PPR
        # java -jar target/Thetis.0.1.jar search --search-mode ppr --hashmap-dir $index_dir \
        # --query-file $query_path --table-dir $tables_dir \
        # --output-dir $output_dir/${mode}/$wikipage/ \
        # --minThreshold $min_threshold --numParticles $num_particles --topK $topK --pprSingleRequestForAllQueryTuples
    done
done