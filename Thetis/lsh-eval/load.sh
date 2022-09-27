#!/bin/bash

INDEX_DIR="/data/cikm/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"

for B in {150,300} ; \
do
    mkdir -p ${INDEX_DIR}buckets_${B}_bandsize_4_vectors_32
done

for INDEX_PATH in ${INDEX_DIR}* ;\
do
    SPLIT=(${INDEX_PATH//_/ })
    BUCKETS=${SPLIT[-5]}
    echo "Loading for "${BUCKETS}" buckets..."
    echo

    java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir ${TABLES} \
        --output-dir ${INDEX_PATH} -t 4 -pv 32 -bs 4
done
