#!/bin/bash

INDEX_DIR="/src/lsh-eval/indexes/"
TABLES="/src/lsh-eval/tables/redirect/"
OUTPUT_DIR='/src/lsh-eval/results/vote_1/'
QUERIES_DIR="/src/lsh-eval/queries/"

for I in ${INDEX_DIR}* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=${OUTPUT_DIR}types/vectors_${VECTORS}/bandsize_${BAND_SIZE}
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
    BAND_SIZE=${SPLIT[-1]}
    OUT=${OUTPUT_DIR}embeddings/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS: "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
    done
done

for I in "/src/lsh-eval/aggregation/"* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=/src/lsh-eval/results/agregation/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS (COLUMN AGGREGATION): "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
    done
done

for I in "/src/lsh-eval/aggregation/"* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=/src/lsh-eval/results/agregation/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS (COLUMN AGGREGATION): "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
    done
done

OUTPUT_DIR='/src/lsh-eval/results/baseline/'

echo "BASELINE"
OUT=${OUTPUT_DIR}baseline_jaccard/vectors_32
mkdir -p ${OUT}

for TOP_K in {10,100} ; \
do
    OUT_K=${OUT}/${TOP_K}
    mkdir -p ${OUT_K}

    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_32_bandsize_8 \
        -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
done

OUT=${OUTPUT_DIR}baseline_cosine/vectors_32
mkdir -p ${OUT}

for TOP_K in {10,100} ; \
do
    OUT_K=${OUT}/${TOP_K}
    mkdir -p ${OUT_K}

    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_32_bandsize_8 \
        -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
done
