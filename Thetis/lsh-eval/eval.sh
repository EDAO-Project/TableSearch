#!/bin/bash

#set -e

INDEX_DIR="/src/lsh-eval/indexes/"
TABLES="/data/tables/SemanticTableSearchDataset/table_corpus/corpus/"
OUTPUT_DIR='/src/lsh-eval/results/vote_3/'
QUERIES_DIR="/src/lsh-eval/queries/"
#TOP_K=100
TOP_K=200

for I in ${INDEX_DIR}* ; \
do
    SPLIT=(${I//_/ })
    VECTORS=${SPLIT[-3]}
    BAND_SIZE=${SPLIT[-1]}
    OUT=${OUTPUT_DIR}types/vectors_${VECTORS}/bandsize_${BAND_SIZE}
    mkdir -p ${OUT}

    echo "PERMUTATION VECTORS: "${VECTORS}
    echo

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} -prop types \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
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

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} -prop embeddings \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
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

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} -prop types \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
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

    for TUPLES in {1,5} ; \
    do
        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
        mkdir -p ${OUT_K}

        QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${I} -prop embeddings \
            -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
    done
done

#OUTPUT_DIR='/src/lsh-eval/results/baseline/'

#echo "BASELINE - PURE BRUTE FORCE"
#OUT=${OUTPUT_DIR}baseline_jaccard
#mkdir -p ${OUT}

#for TUPLES in {1,5} ; \
#do
#    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
#    mkdir -p ${OUT_K}

#    QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
#    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 -prop types \
#        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
#done

#OUT=${OUTPUT_DIR}baseline_cosine
#mkdir -p ${OUT}

#for TUPLES in {1,5} ; \
#do
#    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
#    mkdir -p ${OUT_K}

#    QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
#    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 -prop embeddings \
#        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
#done

#echo "BASELINE - BM25 PRE-FILTERING (TYPES)"
#OUT=${OUTPUT_DIR}baseline_bm25_prefiltering/types
#mkdir -p ${OUT}

#for TUPLES in {1,5} ; \
#do
#    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
#    mkdir -p ${OUT_K}

#    QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
#    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 -prop types \
#        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf BM25 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
#done

#echo "BASELINE - BM25 PRE-FILTERING (EMBEDDINGS)"
#OUT=${OUTPUT_DIR}baseline_bm25_prefiltering/embeddings
#mkdir -p ${OUT}

#for TUPLES in {1,5} ; \
#do
#    OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
#    mkdir -p ${OUT_K}

#    QUERIES=${QUERIES_DIR}${TUPLES}-tuple/
#    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_30_bandsize_10 -prop types \
#        -q ${QUERIES} -td ${TABLES} -od ${OUT_K} -t 4 -pf BM25 --singleColumnPerQueryEntity --useMaxSimilarityPerColumn --embeddingSimilarityFunction norm_cos
#done

#echo "BASELINE - BM25"
#OUT=${OUTPUT_DIR}baseline_bm25
#INDEX_NAME='bm25'
#mkdir -p ${OUT}

#for TOP_K in {10,100} ; \
#do
#    for TUPLES in {1,5} ; \
#    do
#        OUT_K=${OUT}/${TOP_K}/${TUPLES}-tuple/
#        mkdir -p ${OUT_K}

#        QUERIES=${QUERIES_DIR}bm25_${TUPLES}-tuple/queries.txt
#        python3 bm25/pool_ranker.py --output_dir ${OUT_K} \
#            --input_dir ${QUERIES} --index_name ${INDEX_NAME} --topn ${TOP_K}
#    done
#done
