# Command to run bm25_evaluation.py

# Parameters for the wikipages test dataset
# output_df_path='evaluation_dataframes/wikipages_test_dataset/bm25_dfs_dict.pickle'
# output_df_path_remove_query_tables='evaluation_dataframes/wikipages_test_dataset/bm25_remove_query_wikipage_tables_dfs_dict.pickle'
# query_dfs_dir='../../data/queries/wikipages/query_dataframes/wikipages_test_dataset/filtered_queries/'
# scores_dir='../../Web-Table-Retrieval-Benchmark/data/wikipages_testing/ranking/filtered_queries/'
# full_df='../../data/tables/wikipages/wikipages_expanded_dataset/wikipages_df.pickle'
# groundtruth_relevance_scores_dir='../../data/queries/wikipages/groundtruth_generation/wikipage_relevance_scores/wikipages_testing_dataset/jaccard_categories_new/'
# tables_dir='../../data/tables/wikipages/wikipages_test_dataset/tables/'
# remove_query_tables_from_evaluation_mode='remove_query_wikipage_tables'

# Parameters for the wikipages expanded dataset
output_df_path='evaluation_dataframes/wikipages_expanded/bm25_dfs_dict.pickle'
output_df_path_remove_query_tables='evaluation_dataframes/wikipages_expanded/bm25_remove_query_wikipage_tables_dfs_dict.pickle'
query_dfs_dir='../../data/queries/wikipages/query_dataframes/expanded_wikipages/filtered_queries/'
scores_dir='../../Web-Table-Retrieval-Benchmark/data/wikipages_expanded/ranking/filtered_queries/'
full_df='../../data/tables/wikipages/wikipages_expanded_dataset/wikipages_df.pickle'
groundtruth_relevance_scores_dir='../../data/queries/wikipages/groundtruth_generation/wikipage_relevance_scores/wikipages_expanded_dataset/jaccard_categories_new/'
tables_dir='../../data/tables/wikipages/wikipages_expanded_dataset/tables/'
remove_query_tables_from_evaluation_mode='remove_query_wikipage_tables'

# Run BM25 evaluation over all tables
python bm25_evaluation.py --output_df_path $output_df_path \
--query_dfs_dir $query_dfs_dir --scores_dir $scores_dir --full_df $full_df \
--groundtruth_relevance_scores_dir $groundtruth_relevance_scores_dir \
--tables_dir $tables_dir

# Run BM25 evaluation by removing the query tables 
python bm25_evaluation.py --output_df_path $output_df_path_remove_query_tables \
--query_dfs_dir $query_dfs_dir --scores_dir $scores_dir --full_df $full_df \
--groundtruth_relevance_scores_dir $groundtruth_relevance_scores_dir \
--tables_dir $tables_dir --remove_query_tables_from_evaluation_mode $remove_query_tables_from_evaluation_mode
