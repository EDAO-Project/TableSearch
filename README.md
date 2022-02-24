# TableSearch

## Table Search Algorithm based on Semantic Relevance

Semantically Augmented Table Search


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
   
   Take a look at: https://gist.github.com/kuzeko/7ce71c6088c866b0639c50cf9504869a for more details on setting up Neo4J


### Table Datasets

The Table datasets consist of:

- **WikiTables** Tables taken from Wikipedia pages
- **WikiPages** Tables taken from Wikipedia pages with multiple tables in them. This dataset is a subset of the WikiTables dataset
- **GitTables** maybe TODO? 

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
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.6-jdk-11-slim  
   cd /src
   mvn package
   
   # From inside docker
   java -jar target/Thetis.0.1.jar  index --table-type wikitables --table-dir  /data/tables/wikitables/files/wikitables_parsed/tables_10_MAX/ --output-dir /data/index/wikitables/
   ```

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

Run PPR over all queries in `www18_wikitables/queries/`
```bash
./run_www18_wikitable_queries_ppr.sh
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
python generate_queries.py --wikipages_df ../../tables/wikipages/wikipages_df.pickle \
--tables_dir ../../tables/wikipages/tables/ --q_output_dir queries/ \
--wikilink_to_entity ../../index/wikitables/wikipediaLinkToEntity.json --tuples_per_query all
```

### WikiPages Indexing and Search
The following commands should be run inside docker
```bash
# Construct the Index
java -jar target/Thetis.0.1.jar  index --table-type wikitables --table-dir  /data/tables/wikipages/wikipages_dataset/tables/ --output-dir /data/index/wikipages/


```



## OLD WRITEUP (REMOVE EVENTUALLY)
Small Dataset Baseline (Single Column per Query Entity using pre-trained embeddings)
```bash
java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/small_test/ --query-file ../data/queries/test_queries/query_small_test.json --table-dir /data/tables/wikitables/small_test/ --output-dir /data/search/small_test/single_column_per_entity/ --singleColumnPerQueryEntity --usePretrainedEmbeddings
```

Full Dataset Baseline (Single Column per Query Entity)
```bash
java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/www18_wikitables/ --query-file ../data/queries/www18_wikitables/queries/q_9.json --table-dir /data/tables/wikitables/files/www18_wikitables_parsed/tables_10_MAX/ --output-dir /data/search/www18_wikitables/full_index/naive/q_9 --singleColumnPerQueryEntity
```

Full Dataset PPR
```bash
java -jar target/Thetis.0.1.jar search --search-mode ppr --query-file ../data/queries/www18_wikitables/queries/q_15.json --table-dir /data/tables/wikitables/files/www18_wikitables_parsed/tables_10_MAX/ --output-dir /data/search/www18_wikitables/ppr/q_15/ --minThreshold 0.002 --numParticles 300 --topK 200
```

Testing commands (TODO: Delete for final version)


<!-- Table Search Test on www18_wikitables_test using pre-trained embeddings on query q_9 -->
```bash
java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/www18_wikitables_test/ --query-file ../data/queries/www18_wikitables/queries/q_9.json --table-dir /data/tables/wikitables/files/www18_wikitables_parsed_test/tables_10_MAX/ --output-dir /data/search/www18_wikitables_test/single_column_per_entity/ --singleColumnPerQueryEntity --usePretrainedEmbeddings
```

<!-- Table Search Test on www18_wikitables_test using PPR on query q_9 -->
```bash
java -jar target/Thetis.0.1.jar search --search-mode ppr --hashmap-dir ../data/index/www18_wikitables/ --query-file ../data/queries/www18_wikitables/queries/q_9.json --table-dir /data/tables/wikitables/files/www18_wikitables_parsed/tables_10_MAX/ --output-dir /data/search/www18_wikitables_test/ppr_weighted/ --weightedPPR --minThreshold 0.005 --numParticles 300 --topK 200 
```

<!-- Table Search Test on wikitables_small_index using PPR on query test_queries/query_small_test.json -->
```bash
java -jar target/Thetis.0.1.jar search --search-mode ppr --hashmap-dir ../data/index/wikitables_small_test/ --query-file ../data/queries/test_queries/query_small_test.json --table-dir /data/tables/wikitables/small_test/ --output-dir /data/search/small_test/ppr_unweighted_single_q_tuple/ --pprSingleRequestForAllQueryTuples --weightedPPR --minThreshold 0.01 --numParticles 200 --topK 200
```

<!-- Table Search Test on www18_wikitables using PPR on query www18_wikitables/wikipage_tables_analysis/queries/query.json -->
```bash
java -jar target/Thetis.0.1.jar search --search-mode ppr --hashmap-dir ../data/index/www18_wikitables/ --query-file ../data/queries/www18_wikitables/wikipage_tables_analysis/queries/query.json --table-dir /data/tables/wikitables/files/www18_wikitables_parsed/tables_10_MAX/ --output-dir /data/search/wikipage_tables_analysis/ --minThreshold 0.005 --numParticles 300 --topK 200
```

* Perform Search using the Web Interface (TODO: Maybe remove this since we don't use it?)

To test the interface on your local computer (i.e. LOCALHOST) we first need to create an ssh tunnel between the server and your current machine.
SparkJava uses port 4567 by default.
To create the ssh tunnel run the following command:
```
ssh -L 4567:localhost:4567 ubuntu@130.226.98.8
```
Then we can initialize the SparkJava web service

To return results based on PPR run
```bash
java -jar target/Thetis.0.1.jar web --mode ppr --table-dir /data/tables/wikitables/files/tables_50_MAX/ --output-dir /data/index/wikitables/
```

To return results using the baseline run
```bash
java -jar target/Thetis.0.1.jar web --mode analogous --table-dir /data/tables/wikitables/small_test/ --output-dir /data/index/small_test/
```

Then once the server is running simply visit http://localhost:4567/ in your browser and the web interface should show up where you can input your queries.


#### Tough Tables

> Cutrona, V., Bianchi, F., Jimenez-Ruiz, E. and Palmonari, M. (2020). Tough Tables: Carefully Evaluating Entity Linking for Tabular Data. ISWC 2020, LNCS 12507, pp. 1–16.


1. Download from Zenodo URL

  ```bash
  mkdir -P data/tables/2t
  wget  -P data/tables/2t https://zenodo.org/record/4246370/files/2T.zip?download=1 -O 2T.zip
  unzip data/tables/2t/2T.zip
  rm data/tables/2t/2T.zip
  mv data/tables/2t/2T/tables data/tables/2t/files
  rm -v data/tables/2t/files/*Noise*
  mv data/tables/2t/2T/tables
  ```

2. Run preprocessing script for indexing

## DBpedia RDF Embeddings

Generate RDF embeddings by following the steps in the README in the <a href="https://github.com/EDAO-Project/DBpediaEmbedding">DBpediaEmbedding</a> repository. 
Create a folder `embeddings` in `data`. Move the embeddings file `vectors.txt` into the `data/embeddings` folder.

The embeddings will be loaded into an SQLite database with a relation of two attributes in the working directory: _entityIRI_ of type string and _embedding_ of type float[] (float array).

Build the project and run the _embedding_ command from within the container
```
docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.6-jdk-11-slim  
cd /src
mvn package
java -jar target/Thetis.0.1.jar embedding -f /data/embeddings/vectors.txt
```

# Useful Docker Commands

Detach a container (i.e., container still exists after execution and can be connected to again in the future):
* `Ctrl`+`P` and then `Ctrl` + `Q`

Attach to existing container (e.g., after detaching from a container use the following command to connect to it again):
* `docker attach [container_name]`

# Useful Neo4j Commands

Count the number of edges for node `http://dbpedia.org/resource/Harry_Potter`:
* `bin/cypher-shell -u neo4j -p 'admin' "MATCH (a:Resource) WHERE a.uri in ['http://dbpedia.org/resource/Harry_Potter'] RETURN apoc.node.degree(a)"`
