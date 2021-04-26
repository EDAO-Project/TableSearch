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

1. Enter the `data/kg/dbpedia` folder and download the files with the command 

   ```bash
   ./download-dbpedia.sh dbpedia_files.txt 
   ```

2. Load the data into a database: Let's try Neo4j

   https://gist.github.com/kuzeko/7ce71c6088c866b0639c50cf9504869a


### Tables

The Table datasets consist of:

- **WikiTables** from Wikipedia pages
- **SemTabEval** maybe ?
- **Tough Tables** extended SemTabEval dataset 


#### WikiTable

> Bhagavatula, C. S., Noraset, T., & Downey, D. (2015, October). TabEL: entity linking in web tables. In International Semantic Web Conference (pp. 425-441). Springer, Cham.

1. Download from the official link

   ```bash
   mkdir -p data/tables/wikitables
   
   wget -P data/tables/wikitables http://websail-fe.cs.northwestern.edu/TabEL/tables.json.gz
   wget -P data/tables/wikitables http://websail-fe.cs.northwestern.edu/TabEL/tableMentions.json.gz
   ```
  
2. Run preprocessing script for extracting tables

   ```bash
   mkdir -p data/tables/wikitables/files
   cd data/tables/wikitables
   python3 -m venv .virtualenv
   source .virtualenv/bin/activate
   pip install -r requirements.txt
   python extract-tables.py -t tables.json.gz --min-rows 50 --max-rows 0 --min-cols 3 -o files/ 

   # This below does not work yet, ignore
   # python ./extract-table-mentions.py -i ./tableMentions.json.gz -o ./files 
   ```

3. Run preprocessing script for indexing

   ```bash
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.6-jdk-11-slim  
   cd /src
   mvn package
   
   # From inside docker
   java -jar target/Thetis.0.1.jar  index --table-type wikitables --table-dir  /data/tables/wikitables/small_test/ --output-dir /data/index/small_test/
   ```
### Wikitable Search

* Perform Search using the command line

   Small Dataset Baseline
   ```bash
   java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/small_test/ --query-file ../data/queries/query_small_test.json --table-dir /data/tables/wikitables/small_test/ --output-dir /data/index/small_test/
   ```

   Full Dataset Baseline
   ```bash
   java -jar target/Thetis.0.1.jar search --search-mode analogous --hashmap-dir ../data/index/wikitables/ --query-file ../data/queries/query_tuple_large.json --table-dir /data/tables/wikitables/files/tables_50_MAX/ --output-dir /data/index/wikitables/
   ```

   Full Dataset PPR
   ```bash
   java -jar target/Thetis.0.1.jar search --search-mode ppr --query-file ../data/queries/query_tuple.json --table-dir /data/tables/wikitables/files/tables_50_MAX/ --output-dir /data/index/wikitables/
   ```

* Perform Search using the Web Interface

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
