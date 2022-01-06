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
- **GitTables** maybe TODO? 

#### WikiTables
The WikiTables corpus originates from the TabEL paper
> Bhagavatula, C. S., Noraset, T., & Downey, D. (2015, October). TabEL: entity linking in web tables. In International Semantic Web Conference (pp. 425-441). Springer, Cham.

We use the WikiTables corpus as provided in the STR paper (this is the same corpus as described in TabEL but with different filenames so we can appropriately compare our method to STR) 
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
   cd data/tables/wikitables
   python3 -m venv .virtualenv
   source .virtualenv/bin/activate
   pip install -r requirements.txt

   # Create one json file for each table in the wikitables_raw/ directory and  
   python extract_tables.py --input_dir_raw files/wikitables_raw/ --output_dir_clean files/wikitables/

   # Parse each json file to extract the appropriate json format for each table in the dataset
   python extract_tables.py --input_dir files/wikitables/ --output files/wikitables_parsed/ --min-rows 10 --max-rows 0 --min-cols 2   
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

### Wikitable Search

* Perform Search using the command line

   Small Dataset Baseline (Single Column per Query Entity)
   ```bash
   java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/small_test/ --query-file ../data/queries/test_queries/query_small_test.json --table-dir /data/tables/wikitables/small_test/ --output-dir /data/search/small_test/single_column_per_entity/ --singleColumnPerQueryEntity
   ```

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


# Useful Docker Commands

Detach a container (i.e., container still exists after execution and can be connected to again in the future):
* `Ctrl`+`P` and then `Ctrl` + `Q`

Attach to existing container (e.g., after detaching from a container use the following command to connect to it again):
* `docker attach [container_name]`

# Useful Neo4j Commands

Count the number of edges for node `http://dbpedia.org/resource/Harry_Potter`:
* `bin/cypher-shell -u neo4j -p 'admin' "MATCH (a:Resource) WHERE a.uri in ['http://dbpedia.org/resource/Harry_Potter'] RETURN apoc.node.degree(a)`