#!/bin/bash

set -e

INDEX_DIR="/src/lsh-eval/index_new_corpus/"
TABLES="/data/tables/wikitables-2019/corpus/wikitables-2019/"
OUTPUT_DIR="/src/lsh-eval/results_new/vote_3/"
QUERIES_DIR="/src/lsh-eval/queries/"
TOP_K=200

VECTORS=30
BAND_SIZE=10
OUT=${OUTPUT_DIR}types/vectors_${VECTORS}/bandsize_${BAND_SIZE}
mkdir -p ${OUT}

for TUPLES in {1,5} ; \
do
    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
    mkdir -p ${OUT_K}

    QUERIES=${QUERIES_DIR}${TUPLES}-tuple_new/
    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} -prop types \
        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
done

OUT=${OUTPUT_DIR}embeddings/vectors_${VECTORS}/bandsize_${BAND_SIZE}
mkdir -p ${OUT}

for TUPLES in {1,5} ; \
do
    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
    mkdir -p ${OUT_K}

    QUERIES=${QUERIES_DIR}${TUPLES}-tuple_new/
    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} -prop embeddings \
        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
done
