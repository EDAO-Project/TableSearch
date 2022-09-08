#!/bin/bash

set -e

INDEX_DIR="/data/cikm/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"

for B in {150,300} ; \
do
    mkdir -p ${INDEX_DIR}buckets_${B}_bandsize_0_5_permutations_15
done

for INDEX_PATH in ${INDEX_DIR}* ;\
do
    SPLIT=(${INDEX_PATH//_/ })
    BUCKETS=${SPLIT[-6]}
    echo "Loading for "${BUCKETS}" buckets..."
    echo

    java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir ${TABLES} \
        --output-dir ${INDEX_PATH} -t 4 -pv 15 -bf 0.5 -bc ${BUCKETS}
done
