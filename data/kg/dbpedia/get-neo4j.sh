#!/bin/bash
ulimit -n 65535
NEO4J_VERSION=4.1.4
rm -rf neo4j-server
wget https://neo4j.com/artifact.php?name=neo4j-community-${NEO4J_VERSION}-unix.tar.gz -O neo4j.tar.gz
tar xf neo4j.tar.gz
mv neo4j-community-${NEO4J_VERSION} neo4j-server
rm neo4j.tar.gz


export NEO4J_HOME=${PWD}/neo4j-server
export NEO4J_DATA_DIR=${NEO4J_HOME}/data


rm -rf $NEO4J_DATA_DIR

# APOC_VERSION=4.1.0.2
# APOC_FILE=apoc-${APOC_VERSION}-core.jar
# there is a difference between `core` and `all`
# In theory we don't need this, since
# apoc-4.1.0.2-core.jar contains a subset of the functionality and will be bundled from Neo4j 4.1.1
#if [ ! -f ${NEO4J_HOME}/plugins/${APOC_FILE} ]
#then
#    echo "Downloading Neo4j APOC plugin..."
#    wget -P ${NEO4J_HOME}/plugins/ https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/${APOC_VERSION}/${APOC_FILE}
#fi
# Do we need the following?
echo "Installing Neo4j APOC plugin..."
echo 'dbms.security.procedures.unrestricted=apoc.*'  >> ${NEO4J_HOME}/conf/neo4j.conf
echo 'apoc.export.file.enabled=true'  >> ${NEO4J_HOME}/conf/neo4j.conf
echo 'apoc.import.file.use_neo4j_config=false' >> ${NEO4J_HOME}/conf/neo4j.conf

NEOSEM_VERSION=4.1.0.1
NEOSEM_FILE=neosemantics-${NEOSEM_VERSION}.jar

if [ ! -f ${NEO4J_HOME}/plugins/${NEOSEM_FILE} ]
then
    echo "Downloading Neo4j RDF plugin..."
    wget -P ${NEO4J_HOME}/plugins/ https://github.com/neo4j-labs/neosemantics/releases/download/${NEOSEM_VERSION}/${NEOSEM_FILE}
fi
echo "Installing Neo4j RDF plugin..."
echo 'dbms.unmanaged_extension_classes=n10s.endpoint=/rdf' >> ${NEO4J_HOME}/conf/neo4j.conf


sed -i 's/#dbms.default_listen_address/dbms.default_listen_address/' ${NEO4J_HOME}/conf/neo4j.conf

${NEO4J_HOME}/bin/neo4j start
sleep 10


$NEO4J_HOME/bin/neo4j-admin set-initial-password admin


$NEO4J_HOME/bin/neo4j restart
sleep 10


echo "Creating index"
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE;"

${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' 'call n10s.graphconfig.init( { handleMultival: "OVERWRITE",  handleVocabUris: "SHORTEN", keepLangTag: false, handleRDFTypes: "NODES" })'


echo Neo4j log:
tail -n 12 $NEO4J_HOME/logs/neo4j.log

