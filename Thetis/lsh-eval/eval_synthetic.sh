#!/bin/bash

set -e

INDEX_DIR="/src/lsh-eval/indexes_synthetic/"
TABLES="/data/tables/synthetic_corpus/"
OUTPUT_DIR='/src/lsh-eval/results_synthetic/vote_3/'
QUERIES_DIR="/src/lsh-eval/queries/"
TOP_K=100
VECTORS=30
BAND_SIZE=10

for CORPUS_SIZE in {500000,1000000,full}
do
    CORPUS="${TABLES}tables_${CORPUS_SIZE}"
    OUT=${OUTPUT_DIR}corpus_${CORPUS_SIZE}/types/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}${CORPUS_SIZE}/ -prop types \
            -q ${QUERIES} -td ${CORPUS} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
    done

    OUT=${OUTPUT_DIR}corpus_${CORPUS_SIZE}/embeddings/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}${CORPUS_SIZE}/ -prop embeddings \
            -q ${QUERIES} -td ${CORPUS} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
    done
done
