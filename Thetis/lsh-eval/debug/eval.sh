#!/bin/bash

set -e

INDEXES="lsh-eval/debug/indexes"
TABLES="lsh-eval/debug/tables"
QUERIES="lsh-eval/debug/queries"
OUTPUT="lsh-eval/debug/output"
mkdir -p ${OUTPUT}

java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 1000 -i ${INDEXES} \
    -q ${QUERIES} -td ${TABLES} -od ${OUTPUT} -t 4 -pf LSH_TYPES --singleColumnPerQueryEntity
    --adjustedJaccardSimilarity --useMaxSimilarityPerColumn
