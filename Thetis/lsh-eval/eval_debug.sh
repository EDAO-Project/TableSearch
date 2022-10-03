#!/bin/bash

INDEX_DIR="/src/lsh-eval/indexes/"
TABLES="/src/lsh-eval/tables/redirect/"
OUTPUT_DIR='/src/lsh-eval/results/'
QUERIES_DIR="/src/lsh-eval/queries/"

for I in ${INDEX_DIR}* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    OUT=${OUTPUT_DIR}types/vectors_${VECTORS}
    mkdir -p ${OUT}

    echo "PERMUTATION VECTORS: "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
    done
done

for I in ${INDEX_DIR}* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    OUT=${OUTPUT_DIR}embeddings/vectors_${VECTORS}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS: "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
    done
done

OUT=${OUTPUT_DIR}baseline/vectors_32
mkdir -p ${OUT}

for TOP_K in {10,100} ; \
do
    OUT_K=${OUT}/${TOP_K}
    mkdir -p ${OUT_K}

    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_32_bandsize_4 \
        -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
done
