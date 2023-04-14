# TableSearch

## Table Search Algorithm based on Semantic Relevance

Semantically Augmented Table Search.

### Outline

1. Input: Set of tables & and a KG

2. Preprocessing: 
    
   Take tables and output an index that has <tableId, rowId, cellId, uriToEntity>

3. On-Line:
    
   Take input a set of entity tuples:
   
        <Entity1, Entity2>
        <Entity3, Entity4>

   Return a set of ranked tables
        T1, T2, T3 --> ranked based on relevance score

## Data Preparation

### KG

The reference KG is DBpedia.

1. Enter the `data/kg/dbpedia` directory and download the files with the command 

   ```bash
   ./download-dbpedia.sh dbpedia_files.txt 
   ```

2. Load the data into a database. In this case Neo4j

   2.1. Make sure to have JDK 11 installed

    ```bash
    apt-get install default-jdk
    ```
    
    Otherwise, consider using docker : https://hub.docker.com/_/openjdk.
    Third option, not recommended, you can install Java in userspace, you will have to play around with terminal configuration. Here is a starting point under "Installing OpenJDK Manually": https://dzone.com/articles/installing-openjdk-11-on-ubuntu-1804-for-real

   2.2. Get Neo4j v4.1.X Community server and install Neosemantics plugin, also configure neosemantics and add required index
   
   ```bash
   ./get-neo4j.sh
   ```
   
   2.3. Download DBpedia Files, uncompress, ready to be imported

   ```bash
   ./download-dbpedia.sh dbpedia_files.txt
   ```
   
   2.4. Load the data files Notice 1: DBpedia contains malformed IRIs, I've done my best to exclude those, but still some can pass through. A better solution is needed. Notice 2: DBpedia has multi-valued properties with inconsistent types. At the moment handleMultival: "OVERWRITE" could be an option.
   
   ```bash
   ./import-dbpedia.sh
   ```
   
3. Test data is all right:

   - Count nodes

     ```bash
     ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "MATCH (r:Resource) RETURN COUNT(r)"
     ```
     
   - Count edges
   
     ```bash
     ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "MATCH (r1:Resource)-[l]->(r2:Resource) RETURN COUNT(l)"
     ```
     
   - Distinct relationship types

     ```bash
     ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
     ```
     
   - Example node-edges

     ```bash
     ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "MATCH (r1:Resource)-[l]->(r2:Resource) RETURN r1, l, r2 LIMIT 20"
     ```

### Embeddings

Generate RDF embeddings by following the steps in the README in the <a href="https://github.com/EDAO-Project/DBpediaEmbedding">DBpediaEmbedding</a> repository. 
Create a folder `embeddings` in `data`. Move the embeddings file `vectors.txt` into the `data/embeddings` folder.

Pull the Postgress image and setup a database

```
docker pull postgres
docker run -e POSTGRES_USER=<USERNAME> -e POSTGRES_PASSWORD=<PASSWORD> -e POSTGRES_DB=embeddings --name db -d postgres
```

Choose a username and password and substitute `<USERNAME>` and `<PASSWORD>` with them.

Extract the IP address of the Postgress container

```
docker exec db hostname -I
```

With the command `docker exec -it db psql -U thetis embeddings`, you can connect to the `embeddings` database and modify and query it as you like.

Now, start inserting embeddings into Postgres using the IP from above command

```
docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
cd /src
mvn package
java -jar target/Thetis.0.1.jar embedding -f /data/embeddings/vectors.txt -db postgres -h <IP> -p 5432 -dbn embeddings -u <USERNAME> -pw <PASSWORD>
```

Insert the IP address from the previous step instead of `<IP>`.
Add the option `-dp` or `--disable-parsing` to skip pre-parsing the embeddings file before insertion.

### Table Datasets

The Table datasets consist of:

- **WikiTables** Tables taken from Wikipedia pages
- **WikiPages** Tables taken from Wikipedia pages with multiple tables in them. This dataset is a subset of the WikiTables dataset

## WikiTables
The WikiTables corpus originates from the TabEL paper
> Bhagavatula, C. S., Noraset, T., & Downey, D. (2015, October). TabEL: entity linking in web tables. In International Semantic Web Conference (pp. 425-441). Springer, Cham.

We use the WikiTables corpus as provided in the STR paper (this is the same corpus as described in TabEL paper but with different filenames so we can appropriately compare our method to STR) 
>  Zhang, S., & Balog, K. (2018, April). Ad hoc table retrieval using semantic similarity. In Proceedings of the 2018 world wide web conference (pp. 1553-1562).

1. Download the raw corpus and unzip it

   ```bash
   mkdir -p data/tables/wikitables/files/wikitables_raw/
   wget -P data/tables/wikitables http://iai.group/downloads/smart_table/WP_tables.zip

   unzip data/tables/wikitables/WP_tables.zip -d data/tables/wikitables/files/wikitables_raw/
   mv data/tables/wikitables/files/wikitables_raw/tables_redi2_1/* data/tables/wikitables/files/wikitables_raw/
   rm -rf data/tables/wikitables/files/wikitables_raw/tables_redi2_1/
   ```
  
2. Run preprocessing script for extracting tables
   ```bash
   # Create and install dependencies in a python virtual environment
   python3 -m venv .virtualenv
   source .virtualenv/bin/activate
   pip install -r requirements.txt

   cd data/tables/wikitables

   # Create one json file for each table in the wikitables_raw/ directory  
   python extract_tables.py --input_dir_raw files/wikitables_raw/ --output_dir_clean files/wikitables_one_json_per_table/

   # Parse each json file in wikitables_one_json_per_table/ and extract the appropriate json format for each table in the dataset
   # Notice that we also remove tables with less than 10 rows and/or 2 columns
   python extract_tables.py --input_dir files/wikitables_one_json_per_table/ --output files/wikitables_parsed/ --min-rows 10 --max-rows 0 --min-cols 2   
   ```

3. Run preprocessing script for indexing. Notice that we first create a docker container and then run all commands within it

   ```bash
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
   cd /src
   mvn package
   
   # From inside Docker
   java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir  /data/tables/wikitables/files/wikitables_parsed/tables_10_MAX/ --output-dir /data/index/wikitables/ -t 4 -pv 15 -bf 0.2 -bc 20
   ```

   `-pv` is number of permutation vectors for Locality-Sensitive Index (LSH) index of entity types and this number also defines the number of projections in the vector/embedding LSH index. `-bf` is the size of LSH bands defined as the fraction of the signature size of each entity. `-bc` is the number of bucket in the LSH indexes.

4. Materialize table to entity edges in the Graph

   Running the indexing in step 3 will generate the ``tableIDToEntities.ttl`` which contains the mappings of each entity as well as the ``tableIDToTypes.ttl``file.
   Copy these two files to the ``data/kg/dbpedia/files_wikitables/`` directory using:
   ```bash
   mkdir -p data/kg/dbpedia/files_wikitables/
   cp data/index/wikitables/tableIDToEntities.ttl data/index/wikitables/tableIDToTypes.ttl data/kg/dbpedia/files_wikitables/
   ```
      
   We update the neo4j database by introducing table nodes which are connected to all the entities found in them.
   To perform this run the ``generate_table_nodes.sh`` script found in the ``data/kg/dbpedia/`` directory.

### Wikitable Queries (Generate query tuples)
The STR paper is evaluated over 50 keyword queries and for each query a set of tables were labeled as highly-relevant, relevant and not relevant.
Our method is using tuples of entities as query input. For each keyword query in the STR paper we extract a table labeled as highly-relevant that has the largest horizontal mapping of entities (i.e., the table for which the can identify the largest tuple of entities). We can construct the query tuples for each of the 50 keyword queries with the following commands:
```bash
cd data/queries/www18_wikitables/

python generate_queries.py --relevance_queries_path qrels.txt \
--min_rows 10 --min_cols 2 --index_dir ../../index/wikitables/ \
--data_dir ../../tables/wikitables/files/wikitables_parsed/tables_10_MAX/ \
--q_output_dir queries/ --tuples_per_query all \
--filtered_tables_output_dir ../../tables/wikitables/files/wikitables_per_query/ \
--embeddings_path ../../embeddings/embeddings.json
```
Notice that we skip labeled tables that have less than 10 rows and/or less than 2 columns so there will be less than 50 queries after the filtering process.
Also note that the following command will generate a new tables directories at `/tables/wikitables/files/wikitables_per_query/`, one for each query which is used to specify the set of tables the search module will look through. 

### Wikitable Search

In this section we describe how to run our algorithms once all the tables have been indexed.

Run Column-Types Similarity Baseline and Embedding Baseline over all queries in `www18_wikitables/queries/`.
For all baselines each query entity is mapped to a single column.
```bash
# Inside docker run the following script
./run_www18_wikitable_queries.sh
```

## WikiPages
The WikiPages dataset is a subset of the WikiTables dataset.
The WikiPages dataset is constructed by selecting tables from Wikipedia pages that have multiple tables in them.

### Populate the WikiPages dataset
To populate the WikiPages dataset first make sure you finished running all steps outlined for the WikiTables dataset
```bash
cd data/tables/wikipages/

# Extract all the wikipedia pages from the WikiTables dataset and identify the tables in each page
python extract_tables_per_wikipage.py  --input_tables_dir ../wikitables/files/wikitables_parsed/tables_10_MAX/ \
--table_id_to_entities_path ../../index/wikitables/tableIDToEntities.ttl

# Extract the wikipedia pages to use to create the dataset 
python generate_dataset.py --min_num_entities_per_table 10 --min_num_tables_per_page 10 --max_num_tables_per_page 40 \
--wikitables_dir ../wikitables/files/wikitables_parsed/tables_10_MAX/ --output_dir wikipages_dataset/

# Construct the expanded wikipages dataset
python generate_dataset.py --min_num_entities_per_table 10 --min_num_tables_per_page 1 --max_num_tables_per_page 40 \
--wikitables_dir ../wikitables/files/wikitables_parsed/tables_10_MAX/ --output_dir expanded_dataset/
```

### WikiPages query generation
The queries for the WikiPages dataset are generated in a similar fashion as for the Wikitables dataset.
From each selected Wikipedia page we choose the table with the largest horizontal mapping of entities

```bash
cd /data/queries/wikipages/

# Generate the queries for the wikipages dataset
python generate_queries.py --wikipages_df ../../tables/wikipages/wikipages_df.pickle \
--tables_dir ../../tables/wikipages/tables/ --q_output_dir queries/ \
--wikilink_to_entity ../../index/wikitables/wikipediaLinkToEntity.json --tuples_per_query all

# Generate the queries for the expanded wikipages dataset
python generate_queries.py --wikipages_df ../../tables/wikipages/wikipages_expanded_dataset/wikipages_df.pickle \
--tables_dir ../../tables/wikipages/wikipages_expanded_dataset/tables/ \
--q_output_dir queries/expanded_wikipages/minTupleWidth_all_tuplesPerQuery_all/ \
--wikilink_to_entity ../../index/wikipages_expanded/wikipediaLinkToEntity.json \
--output_query_df query_dataframes/expanded_wikipages/minTupleWidth_all_tuplesPerQuery_all.pickle
```

### WikiPages Indexing and Search
The following commands should be run inside docker
```bash
# Construct the Index for the wikipages dataset
java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir  /data/tables/wikipages/wikipages_dataset/tables/ --output-dir /data/index/wikipages/ -t 4 -pv 30 -bs 10

# Construct the index for the expanded wikipages dataset
java -Xmx25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir  /data/tables/wikipages/wikipages_expanded_dataset/tables/ --output-dir /data/index/wikipages_expanded/ -t 4 -pv 30 -bs 10
```

The options `-pv` and `-bs` are LSH parameters and set the number of permutation/projection vectors and band size, respectively.`
