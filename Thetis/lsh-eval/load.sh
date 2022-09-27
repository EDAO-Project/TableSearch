#!/bin/bash

INDEX_DIR="/data/cikm/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"
BANDSIZE=4

for V in {32,64} ;\
do
    mkdir -o ${INDEX_DIR}vectors_${V}_bandsize_${BANDSIZE}
done

for INDEX_PATH in ${INDEX_DIR}* ;\
do
    SPLIT=(${INDEX_PATH//_/ })
    VECTORS=${SPLIT[-3]}
    echo "Loading for "${BUCKETS}" permutation/projection..."
    echo

    java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir ${TABLES} \
        --output-dir ${INDEX_PATH} -t 4 -pv 32 -bs 4
done
