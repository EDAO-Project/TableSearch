#!/bin/bash

set -e

TABLES="lsh-eval/debug/tables"
INDEXES="lsh-eval/debug/indexes"
mkdir -p ${INDEXES}

java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir ${TABLES} \
        --output-dir ${INDEXES} -t 4 -pv 32 -bs 4
