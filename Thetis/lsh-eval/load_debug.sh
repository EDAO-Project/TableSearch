#!/bin/bash

INDEX_DIR="lsh-eval/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"

mkdir -p ${INDEX_DIR}vectors_30_bandsize_10
mkdir ${INDEX_DIR}vectors_128_bandsize_8

for INDEX_PATH in ${INDEX_DIR}* ;\
do
    SPLIT=(${INDEX_PATH//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}

    echo "Loading for "${VECTORS}" permutation/projection..."
    echo

    java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir ${TABLES} \
        --output-dir ${INDEX_PATH} -t 4 -pv ${VECTORS} -bs ${BAND_SIZE}
done
