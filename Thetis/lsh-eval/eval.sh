#!/bin/bash

set -e

INDEX_DIR="/data/cikm/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"
OUTPUT_DIR="/src/lsh-eval/results/"
#QUERIES_DIR="/data/cikm/SemanticTableSearchDataset/queries/"
QUERIES_DIR="queries/"

# Run for each bucket configuration using LSH of entity types
#echo "Evalution of LSH pre-filtering using entity types..."
#echo

#for INDEX_DIR in ${INDEX_DIR}* ; \
#do
#    SPLIT=(${INDEX_DIR//_/ })
#    VECTORS=${SPLIT[-3]}
#    OUT=${OUTPUT_DIR}types/vectors_${VECTORS}
#    mkdir -p ${OUT}

#    echo "PERMUTATION VECTORS: "${VECTORS}
#    echo

#    for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
#    do
#        SPLIT1=(${QUERY_DIR//"/"/ })
#        SPLIT2=(${SPLIT1[-1]//_/ })
#        TUPLES=${SPLIT2[0]}
#        OUT_TUPLES=${OUT}/${TUPLES}_tuple_queries
#        mkdir -p ${OUT_TUPLES}

#        for TOP_K in {10,100} ; \
#        do
#            OUT_K=${OUT_TUPLES}/${TOP_K}
#            mkdir -p ${OUT_K}

#            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} \
#                -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
#        done
#    done
#done

# Run for each bucket configuration using LSH of entity embeddings
#echo "Evalution of LSH pre-filtering using entity embeddings..."
#echo

#for INDEX_DIR in ${INDEX_DIR}* ; \
#do
#    SPLIT=(${INDEX_DIR//_/ })
#    VECTORS=${SPLIT[-3]}
#    OUT=${OUTPUT_DIR}embeddings/vectors_${VECTORS}
#    mkdir -p ${OUT}

#    echo "PROJECTION VECTORS: "${VECTORS}
#    echo

#    for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
#    do
#        SPLIT1=(${QUERY_DIR//"/"/ })
#        SPLIT2=(${SPLIT1[-1]//_/ })
#        TUPLES=${SPLIT2[0]}
#        OUT_TUPLES=${OUT}/${TUPLES}_tuple_queries
#        mkdir -p ${OUT_TUPLES}

#        for TOP_K in {10,100} ; \
#        do
#            OUT_K=${OUT_TUPLES}/${TOP_K}
#            mkdir -p ${OUT_K}

#            java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} \
#                -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --adjustedJaccardSimilarity
#                --useMaxSimilarityPerColumn
#        done
#    done
#done

# Run baseline: Without using pre-filtering
#echo "Evalution of baseline without using LSH pre-filtering..."
#echo

#OUT=${OUTPUT_DIR}baseline/vectors_32
#mkdir -p ${OUT_BUCKETS}

#for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
#do
#    SPLIT1=(${QUERY_DIR//"/"/ })
#    SPLIT2=(${SPLIT1[-1]//_/ })
#    TUPLES=${SPLIT2[0]}
#    OUT_TUPLES=${OUT_BUCKETS}/${TUPLES}_tuple_queries
#    mkdir -p ${OUT_TUPLES}

#    for TOP_K in {10,100} ; \
#    do
#        OUT_K=${OUT_TUPLES}/${TOP_K}
#        mkdir -p ${OUT_K}

#        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}vectors_32_bandsize_4 \
#            -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_K} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
#    done
#done

for INDEX_DIR in ${INDEX_DIR}* ; \
do
    SPLIT=(${INDEX_DIR//_/ })
    VECTORS=${SPLIT[-3]}
    OUT=${OUTPUT_DIR}types/vectors_${VECTORS}
    mkdir -p ${OUT}

    echo "PERMUTATION VECTORS: "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity --useMaxSimilarityP>#        done
done

for INDEX_DIR in ${INDEX_DIR}* ; \
do
    SPLIT=(${INDEX_DIR//_/ })
    VECTORS=${SPLIT[-3]}
    OUT=${OUTPUT_DIR}embeddings/vectors_${VECTORS}
    mkdir -p ${OUT}

    echo "PROJECTION VECTORS: "${VECTORS}
    echo

    for TOP_K in {10,100} ; \
    do
        OUT_K=${OUT}/${TOP_K}
        mkdir -p ${OUT_K}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR} \
            -q ${QUERIES_DIR} -td ${TABLES} -od ${OUT_K} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --adjustedJaccardSimilarity
            --useMaxSimilarityPerColumn
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
