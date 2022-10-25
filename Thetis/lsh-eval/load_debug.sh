#!/bin/bash

INDEX_DIR="lsh-eval/indexes/"
TABLES="lsh-eval/tables/redirect/"

mkdir -p ${INDEX_DIR}vectors_32_bandsize_8
mkdir ${INDEX_DIR}vectors_30_bandsize_6
mkdir ${INDEX_DIR}vectors_30_bandsize_10
mkdir ${INDEX_DIR}vectors_32_bandsize_4
mkdir ${INDEX_DIR}vectors_32_bandsize_16
mkdir ${INDEX_DIR}vectors_128_bandsize_8
mkdir ${INDEX_DIR}vectors_64_bandsize_8

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
