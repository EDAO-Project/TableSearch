#!/bin/bash

INDEX_DIR="/src/lsh-eval/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"
OUTPUT_DIR='/src/lsh-eval/results/vote_3/'
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
        for TUPLES in {1,2} ; \
        do
            OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
            mkdir -p ${OUT_K}

            QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
                -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
        done
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
        for TUPLES in {1,2} ; \
        do
            OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
            mkdir -p ${OUT_K}

            QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
                -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
        done
    done
done

for I in "/src/lsh-eval/indexes_aggregation/"* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=${OUTPUT_DIR}aggregation/types/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS (COLUMN AGGREGATION): "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        for TUPLES in {1,2} ; \
        do
            OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
            mkdir -p ${OUT_K}

            QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
                -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
        done
    done
done

for I in "/src/lsh-eval/indexes_aggregation/"* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=${OUTPUT_DIR}aggregation/embeddings/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS (COLUMN AGGREGATION): "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        for TUPLES in {1,2} ; \
        do
            OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
            mkdir -p ${OUT_K}

            QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} \
                -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
        done
    done
done

OUTPUT_DIR='/src/lsh-eval/results/baseline/'

echo "BASELINE"
OUT=${OUTPUT_DIR}baseline_jaccard
mkdir -p ${OUT}

for TOP_K in {10,100} ; \
do
    for TUPLES in {1,2} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
    done
done

OUT=${OUTPUT_DIR}baseline_cosine
mkdir -p ${OUT}

for TOP_K in {10,100} ; \
do
    for TUPLES in {1,2} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --usePretrainedEmbeddings --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
    done
done
