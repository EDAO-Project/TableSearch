java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir /data/tables/synthetic_corpus/tables_500000/ --output-dir lsh-eval/indexes_synthetic/500000/ -t 4 -pv 30 -bs 10
java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir /data/tables/synthetic_corpus/tables_1000000/ --output-dir lsh-eval/indexes_synthetic/1000000/ -t 4 -pv 30 -bs 10
java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir /data/tables/synthetic_corpus/tables_full/ --output-dir lsh-eval/indexes_synthetic/full/ -t 4 -pv 30 -bs 10
