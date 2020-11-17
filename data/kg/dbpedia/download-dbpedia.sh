#!/bin/bash
set -e

export DATA_DIR="${PWD}/files"
mkdir -p -v "${DATA_DIR}"

if [ "$#" -ne 1 ]; then
    echo "Expected file with list of download URLs"
    exit 1
fi

if [ -d $DATA_DIR ]
then
    echo "Downloading files..."
    # rm -v ${DATA_DIR}/*.* || true
    while read -r line; do
        [[ "$line" =~ ^#.*$ ]] && continue
        wget -P ${DATA_DIR}/ $line
        bzip2 -dk ${DATA_DIR}/${line##*/}
        filename=$(basename -- "${DATA_DIR}/${line##*/}")
        filename="${filename%.*}"
        # Remove corrupted chars and lines
        # iconv -f utf-8 -t ascii -c "${DATA_DIR}/${filename}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' |   grep -v 'xn--b1aew' > ${DATA_DIR}/clean-${filename}
        # rm -v "${DATA_DIR}/${filename}"
    done < $1
else
    echo "No destination folder ${DATA_DIR}"
fi


rm -v ${DATA_DIR}/*.bz2

if [ -f ${DATA_DIR}/'wikipedia-links_lang=en.ttl' ]
then
	echo "Cleaning 'wikipedia-links_lang=en.ttl'"
        grep --color=never -F 'isPrimaryTopicOf' ${DATA_DIR}/'wikipedia-links_lang=en.ttl' > ${DATA_DIR}/'parsed-wikipedia-links_lang=en.ttl'
        rm -v ${DATA_DIR}/'wikipedia-links_lang=en.ttl'
fi
