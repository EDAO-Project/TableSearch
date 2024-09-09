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

First, setup the Thetis Docker environment

```bash
docker build -t thetis .
```

### KG

The reference KG is DBpedia.

1. Enter the `data/kg/dbpedia` directory and download the files with the command 

   ```bash
   ./download-dbpedia.sh dbpedia_files.txt 
   ```

2. Load the data into a database. In this case Neo4j
   
   Take a look at: https://gist.github.com/kuzeko/7ce71c6088c866b0639c50cf9504869a for more details on setting up Neo4J

### Embeddings

Generate RDF embeddings by following the steps in the README in the <a href="https://github.com/EDAO-Project/DBpediaEmbedding">DBpediaEmbedding</a> repository. 
Create a folder `embeddings` in `data`. Move the embeddings file `vectors.txt` into the `data/embeddings` folder.

Enter the project root directory. Pull the Postgress image and setup a database

```
docker pull postgres
docker run -e POSTGRES_USER=<USERNAME> -e POSTGRES_PASSWORD=<PASSWORD> -e POSTGRES_DB=embeddings --name db -d postgres
```

Choose a username and password and substitute `<USERNAME>` and `<PASSWORD>` with them.

Extract the IP address of the Postgress container

```
docker exec -it db hostname -I
```

Remember the IP address for later.
With the command `docker exec -it db psql -U thetis embeddings`, you can connect to the `embeddings` database and modify and query it as you like.

Now, exit the Docker interactive mode and start inserting embeddings into Postgres

```
docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm thetis bash
cd /src
mvn package
java -jar target/Thetis.0.1.jar embedding -f /data/embeddings/vectors.txt -db postgres -h <POSTGRES IP> -p 5432 -dbn embeddings -u <USERNAME> -pw <PASSWORD>
```

Insert the IP address from the previous step instead of `<POSTGRES IP>`.
Add the option `-dp` or `--disable-parsing` to skip pre-parsing the embeddings file before insertion.

### WikiTables Indexing

We use the WikiTables provided in the 

The WikiTables corpus originates from the semantic table search benchmark paper
> Aristotelis Leventidis, Martin Pekár Christensen, Matteo Lissandrini, Laura Di Rocco, Katja Hose, and Renée J. Miller. 2024. A Large Scale Test Corpus for Semantic Table Search. IIn Proceedings of the 47th International ACM SIGIR Conference on Research and Development in Information Retrieval (SIGIR '24). Association for Computing Machinery, New York, NY, USA, 1142–1151.

1. Download the benchmark and unzip the corpus

   ```bash
   git clone https://github.com/dkw-aau/SemanticTableSearchDataset.git && mv SemanticTableSearchDataset data/
   ```
  
2. Follow the data preparation steps in the data/SemanticTableSearchDataset/README.md file (see section _Reproducing Evaluation Results_)

3. Run preprocessing script for indexing. Notice that we first create a docker container and then run all commands within it

   ```bash
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data --network="host" -it --rm thetis bash
   cd /src
   mvn package
   
   # From inside Docker
   java -Xms25g -jar target/Thetis.0.1.jar index --table-dir /data/SemanticTableSearchDataset/table_corpus/tables_2013/ --output-dir /data/index/wikitables/ -t 4
   ```

   The `-t` option specifies the number of threads.

4. Materialize table to entity edges in the Graph

   Running the indexing in step 3 will generate the ``tableIDToEntities.ttl`` which contains the mappings of each entity as well as the ``tableIDToTypes.ttl`` file.
   Copy these two files to the ``data/kg/dbpedia/files_wikitables/`` directory using:
   ```bash
   mkdir -p data/kg/dbpedia/files_wikitables/
   cp data/index/wikitables/tableIDToEntities.ttl data/index/wikitables/tableIDToTypes.ttl data/kg/dbpedia/files_wikitables/
   ```
   
   We update the neo4j database by introducing table nodes which are connected to all the entities found in them.
   To perform this run the ``generate_table_nodes.sh`` script found in the ``data/kg/dbpedia/`` directory.

## Table Search
There are two ways to perform table search: with fully constructed indexes or with partially constructed indexes
In the former, the above indexing steps must be completed.
In the latter, the above indexing steps should not be performed, as this is progressive indexing, where indexes are constructed and queried simultaneously.

### Table Search with Indexes
Table search is only possible if the table corpus has been fully indexed.

1. Run the Docker container and execute a query from within

```bash
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm thetis bash
   java -Xms25g -jar target/Thetis.0.1.jar search -prop types -topK 10 -q /data/SemanticTableSearchDataset/queries/2013/1_tuples_per_query/ -td /data/SemanticTableSearchDataset/table_corpus/tables_2013/ -i /data/index/wikitables/ -od /data/results/ --singleColumnPerQueryEntity --adjustedSimilarity --useMaxSimilarityPerColumn
```

- Instead of `types` for the parameter `-prop`, you can use `predicates` or `embeddings` instead for entity similarity measurements.
-   If you use `embeddings`, then you must specify the cosine similarity function: `--embeddingSimilarityFunction norm_cos`, `--embeddingSimilarityFunction abs_cos`, or `--embeddingSimilarityFunction ang_cos`.
- You can use any value greater than 0 for the `-topK` parameter, which specifies the result set size.
- The `-q` parameter specifies the directory containing the queries.
- The `-td` parameter specifies the directory of the tables to search among.
- The `-i` parameter specifies the directory containing the indexes.
- The `-od` parameter specifies the directory in which to write the result sets.
- You can additionally add a `-pf HNSW` option to use HNSW search space pre-filtering to scale the table search algorithm. Specify neighborhood size with `--hnsw-K <SIZE>`.

2. Find the query results in `data/results/`.

### Progressive Indexes and Table Search
This allows to index the table corpus and perform table search simultaneously.
The table search will be performed over partially constructed indexes, and the indexes will adapt to the search queries.

1. Run the Docker container and start the progressive indexing from within the container

```bash
   docker run -v $(pwd)/queries:/queries -v $(pwd)/new-tables:/tables/ -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm thetis bash
   java -Xms25g -jar target/Thetis.0.1.jar progressive -topK 10 -prop types --table-dir /data/SemanticTableSearchDataset/table_corpus/tables_2013/ --output-dir /data/index/wikitables/ --result-dir /data/results/ --indexing-time 10 --singleColumnPerQueryEntity --adjustedSimilarity --useMaxSimilarityPerColumn
```

- You can additionally add a `-pf HNSW` option to use HNSW search space pre-filtering to scale the table search algorithm.
- The parameter `--indexing-time` allows to specify the amount of time to spend on indexing before a query is executed.
- To execute queries, add the query file(s) to the `queries/` directory.
- To add new tables to the corpus and to be indexed, add the table files to the `new-tables/` directory.
