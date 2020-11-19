#!/bin/bash
export NEO4J_HOME=${PWD}
export NEO4J_IMPORT="${NEO4J_HOME}/neo4j-server/import"
export DATA_IMPORT="${PWD}/files"
export NEO4J_DB_DIR=$NEO4J_HOME/neo4j-server/data/databases/graph.db
ulimit -n 65535

echo "Moving and cleaning"
#cp -r ${DATA_IMPORT}/* ${NEO4J_IMPORT}/
for FILEIN in  ${DATA_IMPORT}/*.ttl
do
    FILE_CLEAN="$(basename "${FILEIN}")"
    iconv -f utf-8 -t ascii -c "${FILEIN}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' > ${NEO4J_IMPORT}/${FILE_CLEAN}
done

echo "Importing"
for file in ${NEO4J_IMPORT}/*.ttl*; do
    # Extracting filename
    # echo $file
    echo ""
    filename="$(basename "${file}")"
    echo "Importing $filename from ${NEO4J_HOME}"
    ${NEO4J_HOME}/neo4j-server/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/$filename\",\"Turtle\");"
    rm -v ${file}
done

echo "Done"
