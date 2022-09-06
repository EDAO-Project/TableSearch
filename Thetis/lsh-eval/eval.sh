#!/bin/bash

set -e

INDEX_DIR="/data/cikm/indexes/"
TABLES="/data/cikm/SemanticTableSearchDataset/table_corpus/corpus/"
OUTPUT_DIR="/src/lsh-eval/results/"
TOP_K=100
QUERIES_DIR="/data/cikm/SemanticTableSearchDataset/queries/"

# Run for each bucket configuration using LSH of entity types
echo "Evalution of LSH pre-filtering using entity types..."
echo

for BUCKET_INDEX_DIR in ${INDEX_DIR}* ; \
do
    SPLIT=(${BUCKET_INDEX_DIR//_/ })
    BUCKETS=${SPLIT[-6]}
    OUT=${OUTPUT_DIR}types/buckets_${BUCKETS}
    mkdir -p ${OUT}

    echo "BUCKETS: "${BUCKETS}
    echo

    for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
    do
        SPLIT1=(${QUERY_DIR//"/"/ })
        SPLIT2=(${SPLIT1[-1]//_/ })
        TUPLES=${SPLIT2[0]}
        OUT_TUPLES=${OUT}/${TUPLES}_tuple_queries
        mkdir -p ${OUT_TUPLES}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${BUCKET_INDEX_DIR} \
            -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_TUPLES} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity --adjustedJaccardSimilarity
    done
done

# Run for each bucket configuration using LSH of entity embeddings
echo "Evalution of LSH pre-filtering using entity embeddings..."
echo

for BUCKET_INDEX_DIR in ${INDEX_DIR}* ; \
do
    SPLIT=(${BUCKET_INDEX_DIR//_/ })
    BUCKETS=${SPLIT[-6]}
    OUT=${OUTPUT_DIR}embeddings/buckets_${BUCKETS}
    mkdir -p ${OUT}

    echo "BUCKETS: "${BUCKETS}
    echo

    for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
    do
        SPLIT1=(${QUERY_DIR//"/"/ })
        SPLIT2=(${SPLIT1[-1]//_/ })
        TUPLES=${SPLIT2[0]}
        OUT_TUPLES=${OUT}/${TUPLES}_tuple_queries
        mkdir -p ${OUT_TUPLES}

        java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${BUCKET_INDEX_DIR} \
            -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_TUPLES} -t 4 -pf LSH_EMBEDDINGS --singleColumnPerQueryEntity --adjustedJaccardSimilarity
    done
done

# Run baseline: Without using pre-filtering
echo "Evalution of baseline without using LSH pre-filtering..."
echo

OUT=${OUTPUT_DIR}baseline
mkdir ${OUT}

for QUERY_DIR in ${QUERIES_DIR}*_tuples_per_query ; \
do
    SPLIT1=(${QUERY_DIR//"/"/ })
    SPLIT2=(${SPLIT1[-1]//_/ })
    TUPLES=${SPLIT2[0]}
    OUT_TUPLES=${OUT}/${TUPLES}_tuple_queries
    mkdir ${OUT_TUPLES}

    java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK ${TOP_K} -i ${INDEX_DIR}buckets_15_bandsize_0_2_permutations_15 \
        -q ${QUERY_DIR} -td ${TABLES} -od ${OUT_TUPLES} -t 4 --singleColumnPerQueryEntity --adjustedJaccardSimilarity
done
