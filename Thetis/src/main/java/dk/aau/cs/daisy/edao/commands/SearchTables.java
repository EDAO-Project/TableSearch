package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.io.FileReader;
import java.io.*;
import java.nio.file.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;

import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.*;
import dk.aau.cs.daisy.edao.loader.IndexReader;
import dk.aau.cs.daisy.edao.search.ExactSearch;
import dk.aau.cs.daisy.edao.search.Search;
import dk.aau.cs.daisy.edao.similarity.JaccardSimilarity;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.IdDictionary;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.utils;
import dk.aau.cs.daisy.edao.utilities.HungarianAlgorithm;
import dk.aau.cs.daisy.edao.utilities.ppr;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import picocli.CommandLine;
import me.tongfei.progressbar.*;

@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {

    //********************* Command Line Arguments *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli
    
    public enum SearchMode {
        EXACT("exact"), ANALOGOUS("analogous"), PPR("ppr");

        private final String mode;
        SearchMode(String mode){
            this.mode = mode;
        }

        public final String getMode(){
            return this.mode;
        }

        @Override
        public String toString() {
            return this.mode;
        }
    }

    public enum QueryMode {
        TUPLE("tuple"), ENTITY("entity");

        private final String mode;
        QueryMode(String mode){
            this.mode = mode;
        }

        public final String getMode(){
            return this.mode;
        }

        @Override
        public String toString() {
            return this.mode;
        }
    }

    public enum EmbeddingSimFunction {
        NORM_COS("norm_cos"), ABS_COS("abs_cos"), ANG_COS("ang_cos"); 

        private final String simFunction;
        EmbeddingSimFunction(String simFunction){
            this.simFunction = simFunction;
        }

        public final String getEmbeddingSimFunction(){
            return this.simFunction;
        }

        @Override
        public String toString() {
            return this.simFunction;
        }
    }

    public enum EmbeddingsInputMode {
        FILE("file"), DATABASE("data"); 

        private final String mode;
        EmbeddingsInputMode(String mode){
            this.mode = mode;
        }

        public final String getEmbeddingsInputMode(){
            return this.mode;
        }

        @Override
        public String toString() {
            return this.mode;
        }
    }

    @CommandLine.Option(names = { "-sm", "--search-mode" }, description = "Must be one of {exact, analogous}", required = true)
    private SearchMode searchMode = null;

    @CommandLine.Option(names = { "-qm", "--query-mode" }, description = "Must be one of {tuple, entity}", required = true, defaultValue = "tuple")
    private QueryMode queryMode = null;

    @CommandLine.Option(names = { "-scpqe", "--singleColumnPerQueryEntity"}, description = "If specified, each query tuple will be evaluated against only one entity")
    private boolean singleColumnPerQueryEntity;

    @CommandLine.Option(names = { "-upe", "--usePretrainedEmbeddings"}, description = "If specified, pre-trained embeddings are used to capture the similarity between two entities whenever possible")
    private boolean usePretrainedEmbeddings;

    @CommandLine.Option(names = { "-pem", "--embeddingsInputMode" }, description = "Specifies the manner by which the preTrainedEmbeddings are loaded from. Must be one of {file, database}", defaultValue = "file")
    private EmbeddingsInputMode embeddingsInputMode = null;

    @CommandLine.Option(names = { "-ajs", "--adjustedJaccardSimilarity"}, description = "If specified, the Jaccard similarity between two entities can only be one if the two entities compared are identical. " + 
        "If two different entities share the same types then assign an adjusted score of 0.95. ")
    private boolean adjustedJaccardSimilarity;

    @CommandLine.Option(names = { "-wjs", "--weightedJaccardSimilarity"}, description = "If specified, the a weighted Jaccard similarity between two entities is performed. " + 
        "The weights for each entity type correspond to their respective IDF scores")
    private boolean weightedJaccardSimilarity;

    @CommandLine.Option(names = { "--useMaxSimilarityPerColumn"}, description = "If specified, instead of taking the average similarity across columns as a score the maximum value is used")
    private boolean useMaxSimilarityPerColumn;

    @CommandLine.Option(names = { "-wppr", "--weightedPPR"}, description = "If specified, the number of particles given to each query node depends on their number of edges and IDF scores.")
    private boolean weightedPPR;

    @CommandLine.Option(names = { "--pprSingleRequestForAllQueryTuples"}, description = "If specified, all entities across all query tuples are treated as a single query tuple. So only one PPR request is used")
    private boolean pprSingleRequestForAllQueryTuples;

    @CommandLine.Option(names = { "-esf", "--embeddingSimilarityFunction" }, description = "The similarity function used to compare two embedding vectors. Must be one of {norm_cos, abs_cos, ang_cos}", required = true, defaultValue="ang_cos")
    private EmbeddingSimFunction embeddingSimFunction = null;

    @CommandLine.Option(names = { "-mt", "--minThreshold"}, description = "The minimum threshold used by PPR", defaultValue="0.005")
    private Double minThreshold;

    @CommandLine.Option(names = { "-np", "--numParticles"}, description = "The number of particles used by PPR", defaultValue="200.0")
    private Double numParticles;

    @CommandLine.Option(names = { "-topK", "--topK"}, description = "The top-k values to be returned when running PPR", defaultValue="100")
    private Integer topK;

    @CommandLine.Option(names = {"-ep", "--embeddingsPath"}, description = "Path to embeddings database for file. Whichever is specified by the `preTrainedEmbeddingsMode` argument")
    private String embeddingsPath = null;

    private File indexDir = null;
    @CommandLine.Option(names = { "-i", "--index-dir" }, paramLabel = "INDEX_DIR", description = "Directory of loaded indexes", defaultValue = "../data/index/wikitables/")
    public void setHashMapDirectory(File value) {
        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--index-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--index-dir': " + "the path does not point to a directory.", value));
        }

        this.indexDir = value;
    }

    private File queryFile = null;
    @CommandLine.Option(names = { "-q", "--query-file" }, paramLabel = "QUERY", description = "Path to the query json file", required = true)
    public void setQueryFile(File value) {
        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--query-file': " + "the directory does not exists.", value));
        }

        if (!value.isFile()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--query-file': " + "the path does not point to a directory.", value));
        }

        queryFile = value;
    }

    private File tableDir = null;
    @CommandLine.Option(names = { "-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing tables", required = true)
    public void setTableDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " + "the path does not point to a directory.", value));
        }
        tableDir = value;
    }

    private File outputDir = null;
    @CommandLine.Option(names = { "-od", "--output-dir" }, paramLabel = "OUT_DIR", description = "Directory where to save the search results", required = true)
    public void setOutputDirectory(File value) {
        outputDir = value;
    }

    private File configFile = null;
    @CommandLine.Option(names = { "-cf", "--config"}, paramLabel = "CONF", description = "configuration file", required = true, defaultValue = "./config.properties" )
    public void setConfigFile(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the file does not exists.", value));
        }

        if (value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the path should point to a file not to a directory.", value));
        }
        configFile = value;
    }

    @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of threads", required = true, defaultValue = "1")
    private int threads;

    private Neo4jEndpoint connector;

    // Initialize a connection with the embeddings Database
    private DBDriverBatch<List<Double>, String> store;

    @Override
    public Integer call() {
        System.out.println("Hashmap Directory: " + this.indexDir);
        System.out.println("Query File: " + this.queryFile);
        System.out.println("Table Directory: " + this.tableDir);    // TODO: Make this redundant - necessary info should be found in indexes
        System.out.println("Output Directory: " + this.outputDir);
        System.out.println("Single Column per Query Entity: " + this.singleColumnPerQueryEntity);

        // Create output directory if it doesn't exist
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        if (this.embeddingsInputMode == EmbeddingsInputMode.DATABASE)
            this.store = Factory.fromConfig(false);

        // Read off the queryEntities list from a json object
        Table<String> queryTable = TableParser.toTable(this.queryFile);

        if (queryTable == null)
        {
            System.out.println("Query file '" + this.queryFile + "' could not be parsed");
            return -1;
        }

        System.out.println("Query Entities: " + queryEntities + "\n");

        try
        {
            // Perform De-Serialization of the indices
            long startTime = System.nanoTime();
            IndexReader indexReader = new IndexReader(this.indexDir, true);
            indexReader.performIO();

            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Indexes loaded from disk in " + elapsedTime / 1e9 + " seconds\n");

            EntityLinking linker = indexReader.getLinker();
            EntityTable entityTable = indexReader.getEntityTable();
            EntityTableLink entityTableLink = indexReader.getEntityTableLink();

            // Ensure all query entities are mappable
            if (this.ensureQueryEntitiesMapping(queryTable, linker.getDictionary(), entityTableLink))
                System.out.println("All query entities are mappable!\n\n");

            else
            {
                System.out.println("NOT all query entities are mappable!");
                return -1;
            }

            // Perform search according to the specified `search-mode`
            System.out.println("Search mode: " + this.searchMode.getMode());

            switch (this.searchMode){
                case EXACT:
                    exactSearch(queryTable, linker, entityTable, entityTableLink);
                    break;

                case ANALOGOUS:
                    analogousSearch();
                    break;

                case PPR:
                    ppr();
                    break;
            }

            if (this.embeddingsInputMode == EmbeddingsInputMode.DATABASE)
                this.store.close();

            return 1;
        }

        catch (IOException e)
        {
            System.out.println("Failed to load indexes from disk");
            return -1;
        }
    }

    //********************* Global Variables *********************//

    // A doubly nested list of strings containing the entities for each tuple. If the query mode is entities the array is still doubly nested but there is only one row with the list of entities.
    private List<List<String>> queryEntities = new Vector<>();

    // Map a wikipedia uri to a dbpedia uri (e.g. https://en.wikipedia.org/wiki/Yellow_Yeiyah -> http://dbpedia.org/resource/Yellow_Yeiyah)
    private Map<String, String> wikipediaLinkToEntity = new HashMap<>(100000);

    // Map a dbpedia uri to its list of rdf__types (e.g. http://dbpedia.org/resource/Yellow_Yeiyah -> [http://dbpedia.org/ontology/Swimmer, http://dbpedia.org/ontology/Person,  http://dbpedia.org/ontology/Athlete])
    private Map<String, List<String>> entityTypes = Collections.synchronizedMap(new HashMap<>());

    // Map an entity (i.e. dbpedia uri) to the list of filenames it is found.  
    // e.g. entityToFilename.get("http://dbpedia.org/resource/Yellow_Yeiyah") = [table-316-3.json, ...]
    private Map<String, List<String>> entityToFilename = new HashMap<>();

    // Map an entity in a filename (the key is the entity_URI + "__" + filename) to the list of [rowId, colId] where the entity is found
    // e.g. entityInFilenameToTableLocations.get("http://dbpedia.org/resource/Yellow_Yeiyah__table-316-3.json") = [[0,2], [2,2], ...]
    private Map<String, List<List<Integer>>> entityInFilenameToTableLocations = new HashMap<>();

    // A triple nested map corresponding to a tablename, rowNumber and query tuple number and mapping to its maximal similarity vector 
    private Map<String, Map<Integer, Map<Integer, List<Double>>>> similarityVectorMap = Collections.synchronizedMap(new HashMap<>());

    // Maps each filename to its relevance score according to the query
    private Map<String, Double> filenameToScore = Collections.synchronizedMap(new HashMap<>());

    // Maps each entity to its IDF score. The idf score of an entity is given by log(N/(1+n_t)) + 1 where N is the number of filenames/tables in the repository
    // and n_t is the number of tables that contain the entity in question.
    private Map<String, Double> entityToIDF = Collections.synchronizedMap(new HashMap<>());

    // Maps each entity type to its IDF score. This Hashmap is only used if `weightedJaccardSimilarity` is used
    public static Map<String, Double> entityTypeToIDF = new HashMap<>();

    // Maps an entity to it's pre-trained embedding.
    private Map<String, List<Double>> entityToEmbedding = new HashMap<>();

    //********************* Global Variables of Statistics *********************//

    private Double elapsedTime = 0.0;

    private Map<String, Map<String, Object>> filenameToStatistics = Collections.synchronizedMap(new HashMap<>());

    private Integer numEmbeddingSimComparisons = 0;
    private Integer numNonEmbeddingSimComparisons = 0;

    private Integer hasEmbeddingCoverageFails = 0;
    private Integer hasEmbeddingCoverageSuccesses = 0;
    private Set<String> queryEntitiesMissingCoverage = new HashSet<String>();


    public boolean ensureQueryEntitiesMapping(Table<String> query, IdDictionary<String> entityDict, EntityTableLink tableLink)
    {
        int rows = query.rowCount();

        for (int i = 0; i < rows; i++)
        {
            Table.Row<String> row = query.getRow(i);
            int rowSize = row.size();

            for (int j = 0; j < rowSize; j++)
            {
                Id entityId = entityDict.get(row.get(j));

                if (!tableLink.contains(entityId))
                    return false;
            }
        }

        return true;
    }

    public void exactSearch(Table<String> query, EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        Iterator<String> entityIter = linker.getDictionary().keys().asIterator();

        while (entityIter.hasNext())
        {
            String entity = entityIter.next();
            Id entityId = linker.getDictionary().get(entity);
            List<String> tableFiles = entityTableLink.find(entityId);

            if (tableFiles == null || tableFiles.isEmpty())
                System.out.println("'" + entity + "' does not map to any known entity in the constructed index");

            else
                System.out.println("There are " + tableFiles.size() + " files that contain the entity '" + entity + "'");
        }

        Search search = new ExactSearch(linker, entityTable, entityTableLink);
        Iterator<Pair<String, Double>> resIter = search.search(query).getResults();

        while (resIter.hasNext())
        {
            Pair<String, Double> result = resIter.next();
            System.out.println("For filename '" + result.getFirst() + "', there are " + result.getSecond() + " matching tuples");
        }

        // Analyze all pairwise combinations of query entities
        List<String> flattenedQueryTable = new ArrayList<>();
        int rows = query.rowCount();

        for (int i = 0; i < rows; i++)
        {
            Table.Row<String> row = query.getRow(i);
            int columns = row.size();

            for (int j = 0; j < columns; j++)
            {
                flattenedQueryTable.add(row.get(j));
            }
        }

        int tableEntities = flattenedQueryTable.size();
        System.out.println("\n2-entities analysis:");

        for (int i = 0; i < tableEntities; i++)
        {
            for (int j = 0; j < tableEntities; j++)
            {
                resIter = search.search(new DynamicTable<>(List.of(flattenedQueryTable))).getResults();

                while (resIter.hasNext())
                {
                    Pair<String, Double> result = resIter.next();
                    System.out.println("For filename '" + result.getFirst() + "', there are " + result.getSecond() + " matching tuples");
                }
            }
        }
    }

    /**
     * Given a list of entities, return a ranked list of table candidates
     */
    public int analogousSearch() {

        // Loop over each table in the tables directory
        try {
            // Get a list of all the files from the specified directory
            Stream<Path> file_stream = Files.find(this.tableDir.toPath(), Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> file_paths_list = file_stream.collect(Collectors.toList());
            System.out.println("There are " + file_paths_list.size() + " files to be processed.");

            long startTime = System.nanoTime();
            ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
            List<Future<Boolean>> parsed = new ArrayList<>(file_paths_list.size());
            // Parse each file (TODO: Maybe parallelise this process? How can the global variables be shared?)
            for (int i=0; i < file_paths_list.size(); i++) {
                Path filePath = file_paths_list.get(i).toAbsolutePath();
                Future<Boolean> future = threadPool.submit(() -> this.searchTable(filePath));
                parsed.add(future);
            }

            long done = 1, prev = 0;

            while (done != file_paths_list.size()) {
                done = parsed.stream().filter(Future::isDone).count();

                if (done - prev >= 100) {
                    System.out.println("Processed " + done + "/" + file_paths_list.size() + " files...");
                    prev = done;
                }
            }

            elapsedTime = (System.nanoTime() - startTime) / 1e9;
            long parsedTables = parsed.stream().filter(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }).count();

            System.out.println("A total of " + parsedTables + " tables were parsed.");
            System.out.println("Elapsed time: " + elapsedTime + " seconds\n");
        } 
        catch (IOException | RuntimeException e) {
            e.printStackTrace();
            return -1;
        }

        System.out.println("Successfully completed search over all tables!\n");

        if (usePretrainedEmbeddings) {
            System.out.println("A total of " + numEmbeddingSimComparisons + " entity comparisons were made using embeddings.");
            System.out.println("A total of " + numNonEmbeddingSimComparisons + " entity comparisons cannot be made due to lack of embeddings.");
            double percentage = (numEmbeddingSimComparisons / ((double)numNonEmbeddingSimComparisons + numEmbeddingSimComparisons)) * 100;
            System.out.println(percentage + "% of required entity comparisons were made using embeddings.\n");


            System.out.println("Embedding Coverage successes: " + hasEmbeddingCoverageSuccesses);
            System.out.println("Embedding Coverage failures: " + hasEmbeddingCoverageFails);
            System.out.println("Embedding Coverage Success Rate: " + (double)hasEmbeddingCoverageSuccesses / (hasEmbeddingCoverageSuccesses + hasEmbeddingCoverageFails));
            System.out.println("Query Entities with missing embedding coverage: " + queryEntitiesMissingCoverage + "\n");
        }

        // Compute a relevance score for each file/table (higher score means more relevant)
        this.getFilenameScores(20, "euclidean");

        this.saveFilenameScores(outputDir);

        return 1;
    }

    /**
     * Given a path to a table, update the similarityVectorMap for the current query with respect
     * to each row and each query tuple in this table
     * 
     * If '--singleColumnPerQueryEntity' is specified then each query tuple can map to only one column in the table
     */
    public boolean searchTable(Path path) {
        JsonTable table = utils.getTableFromPath(path);
        // Check if the table is empty/erroneous
        if (table.numDataRows == 0) {
            return false;
        }

        String filename = path.getFileName().toString();
        Map<String, Object> statisticsMap = new HashMap<>();

        // If each query entity needs to map to only one column find the best mapping
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();
        if (singleColumnPerQueryEntity) {
            tupleToColumnMappings = getQueryToColumnMapping(table);

            // Log in the `statisticsMap` the column names aligned with each query tuple
            List<List<String>> tuple_query_to_column_names = new ArrayList<>();
            for (Integer tupleId=0; tupleId<tupleToColumnMappings.size();tupleId++) {
                tuple_query_to_column_names.add(new ArrayList<String>());
                for (Integer entityId=0; entityId<tupleToColumnMappings.get(tupleId).size(); entityId++) {
                    Integer aligned_col_num = tupleToColumnMappings.get(tupleId).get(entityId);
                    if ((table.headers.size() > aligned_col_num) && (aligned_col_num >= 0)) {
                        // Ensure that `table` has headers that we can index them
                        tuple_query_to_column_names.get(tupleId).add(table.headers.get(aligned_col_num).text);
                    }
                }
            }
            statisticsMap.put("tuple_query_alignment", tuple_query_to_column_names);
        }

        // Map each row number of the table to each query tuple and its respective similarity vector
        Map<Integer, Map<Integer, List<Double>>> rowTupleIDVectorMap = new HashMap<>();

        Integer numEntityMappedRows = 0;    // Number of rows in a table that have at least one cell mapping ot a known entity

        // Loop over each query entity. TODO: Treat this on a tuple by tuple basis
        int rowId = 0;
        // Loop over every cell in a table
        for(List<JsonTable.TableCell> row : table.rows){
            // In a given row map a colId to its respective entity value 
            Map<Integer, String> colIdToEntity = new HashMap<>();
            for (Integer colId=0; colId<row.size(); colId++) {
                if(!row.get(colId).links.isEmpty()) {
                    // A cell value may map to multiple entities. Currently use the first one
                    // TODO: Consider all of them?
                    for(String link : row.get(colId).links) {
                        // Only consider links for which we have a known entity mapping
                        if (wikipediaLinkToEntity.containsKey(link)) {
                            colIdToEntity.put(colId, wikipediaLinkToEntity.get(link));
                            break;
                        }
                    }
                }
            }

            // Compute similarity vectors only for rows that map to at least one entity
            if (!colIdToEntity.isEmpty()) {
                numEntityMappedRows += 1;
                Map<Integer, List<Double>> tupleIDVectorMap = new HashMap<>();

                // For each row and for each query tuple compute the maximal similarity vector
                for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
                    // If pre-trained embeddings are being used, we need to ensure that all entities
                    // of the current query tuple as well as its corresponding row entities are all mappable to known pre-trained embeddings
                    if ( ( usePretrainedEmbeddings && hasEmbeddingCoverage(queryEntities.get(tupleID), colIdToEntity, tupleToColumnMappings, tupleID) ) 
                         || !usePretrainedEmbeddings) {
                    
                        // Initialize the maximum vector for the current tuple, to a zero vector of size equal to the query tuple size.
                        List<Double> maximumTupleVector = new ArrayList<>(Collections.nCopies(queryEntities.get(tupleID).size(), 0.0));

                        for (Integer queryEntityID=0; queryEntityID<queryEntities.get(tupleID).size(); queryEntityID++) {
                            String queryEntity = queryEntities.get(tupleID).get(queryEntityID);
                            Double bestSimScore = 0.0;

                            if (singleColumnPerQueryEntity) {
                                // Each query entity maps to only one entity from a single column (if it exists)
                                Integer assigned_col_id = tupleToColumnMappings.get(tupleID).get(queryEntityID);
                                if (colIdToEntity.containsKey(assigned_col_id)) {
                                    bestSimScore = this.entitySimilarityScore(queryEntity, colIdToEntity.get(assigned_col_id));
                                } 
                            }
                            else {
                                // Loop over each entity in the row
                                for (String rowEntity : colIdToEntity.values()) {
                                    // Compute pairwise entity similarity between 'queryEntity' and 'rowEntity'
                                    Double simScore = this.entitySimilarityScore(queryEntity, rowEntity);
                                    if (simScore > bestSimScore) {
                                        bestSimScore = simScore;
                                    }
                                }
                            }
                            maximumTupleVector.set(queryEntityID, bestSimScore);
                        }
                        tupleIDVectorMap.put(tupleID, maximumTupleVector);
                    } // End of if statement condition
                    else {
                        // TODO: Track how many (tupleID, rowID) pairs were skipped due to lack of embedding mappings
                    }
                }
                rowTupleIDVectorMap.put(rowId, tupleIDVectorMap);
            }
            rowId+=1;
        }

        similarityVectorMap.put(filename, rowTupleIDVectorMap);

        // Update Statistics
        statisticsMap.put("numEntityMappedRows", numEntityMappedRows);
        statisticsMap.put("fractionOfEntityMappedRows", (double)numEntityMappedRows / table.numDataRows);
        filenameToStatistics.put(filename, statisticsMap);
        
        return true;
    }

    /*
     * Given a list of the entities of a query tuple, the entities of a row in the table, the mapping
     * of the table columns to the query entities if any and the id of the query tuple; identify
     * if there exist pre-trained embeddings for each query entity and each matching row entity
     */
    public boolean hasEmbeddingCoverage(List<String> queryEntities, Map<Integer, String> colIdToEntity, List<List<Integer>> tupleToColumnMappings, Integer queryTupleID) {       
        // Ensure that all query entities have an embedding
        for (String qEnt : queryEntities) {
            if (!entityExists(qEnt)) {
                System.out.println("Missing query entity: " + qEnt);
                hasEmbeddingCoverageFails += 1;
                queryEntitiesMissingCoverage.add(qEnt);
                return false;
            }
        }

        // If `singleColumnPerQueryEntity` is true then ensure that all row entities that are
        // in the chosen columns (i.e. tupleToColumnMappings.get(queryTupleID) ) need to be mappable
        List<String> relevant_row_ents = new ArrayList<>();
        if (singleColumnPerQueryEntity) {
            for (Integer assigned_col_id : tupleToColumnMappings.get(queryTupleID)) {
                if (colIdToEntity.containsKey(assigned_col_id)) {
                    relevant_row_ents.add(colIdToEntity.get(assigned_col_id));
                } 
            }
        }
        else {
            // All entities in `rowEntities` are relevant
            relevant_row_ents = new ArrayList<String>(colIdToEntity.values());
        }

        // Loop over all relevant row entities and ensure there is a pre-trained embedding mapping for each one
        for (String rowEnt : relevant_row_ents) {
            if (!entityExists(rowEnt)) {
                hasEmbeddingCoverageFails += 1;
                return false;
            }
        }

        if (relevant_row_ents.isEmpty()) {
            hasEmbeddingCoverageFails += 1;
            return false;
        }

        hasEmbeddingCoverageSuccesses += 1;
        return true;
    }

    /*
     * Map each query entity from each query tuple to its best matching column id for the input 'table'   
     */
    public List<List<Integer>> getQueryToColumnMapping(JsonTable table) {

        // Initialize multi-dimensional array indexed by (tupleID, entityID, columnID) mapping to the 
        // aggregated score for that query entity with respect to the column
        List<List<List<Double>>> entityToColumnScore = new ArrayList<>();
        for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
            entityToColumnScore.add(new ArrayList<List<Double>>(queryEntities.get(tupleID).size()));
            for (Integer queryEntityID=0; queryEntityID<queryEntities.get(tupleID).size(); queryEntityID++) {
                entityToColumnScore.get(tupleID).add(new ArrayList<Double>(Collections.nCopies(table.numCols, 0.0)));
            }
        }
        
        // Loop over every cell in a table and populate 'entityToColumnScore'
        for(List<JsonTable.TableCell> row : table.rows){
            int colId = 0;
            for(JsonTable.TableCell cell : row){
                if(!cell.links.isEmpty()) {
                    String curEntity = null;
                    // A cell value may map to multiple entities. Currently use the first one. TODO: Consider all of them?
                    for(String link : cell.links) {
                        // Only consider links for which we have a known entity mapping
                        if (wikipediaLinkToEntity.containsKey(link)) {
                            curEntity = wikipediaLinkToEntity.get(link);
                            break;
                        }
                    }
                    if (curEntity != null) {
                        // Loop over each query tuple and each entity in a tuple and compute a score between the query entity and 'curEntity'
                        for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
                            for (Integer queryEntityID=0; queryEntityID<queryEntities.get(tupleID).size(); queryEntityID++) {
                                String queryEntity = queryEntities.get(tupleID).get(queryEntityID);
                                Double score = this.entitySimilarityScore(queryEntity, curEntity);
                                entityToColumnScore.get(tupleID).get(queryEntityID).set(colId, entityToColumnScore.get(tupleID).get(queryEntityID).get(colId) + score);
                            }
                        }
                    }
                }
                colId+=1;
            }
        }

        // Find the best mapping between a query entity and a column for each query tuple.
        List<List<Integer>> tupleToColumnMappings = getBestMatchFromScores(entityToColumnScore);
        return tupleToColumnMappings;
    }

    /*
     * Given the multi-dimensional array indexed by (tupleID, entityID, columnID) mapping to
     * the aggregated score for that query entity with respect the column, return the best columnID map for each entity
     * 
     * This function returns a 2-D list of integers indexed by (tupleID, entityID) and maps to the columnID 
     */
    public List<List<Integer>> getBestMatchFromScores(List<List<List<Double>>> entityToColumnScore) {
        
        // Mapping of the matched columnIDs for each entity in each query tuple
        // Indexed by (tupleID, entityID) mapping to the columnID. If a columnID is -1 then that entity is not chosen for assignment
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();

        for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
            // 2-D array where each row is composed of the negative column relevance scores for a given entity in the query tuple
            // Taken from: https://stackoverflow.com/questions/10043209/convert-arraylist-into-2d-array-containing-varying-lengths-of-arrays
            double[][] scoresMatrix = entityToColumnScore.get(tupleID).stream().map(  u  ->  u.stream().mapToDouble(i->-1*i).toArray()  ).toArray(double[][]::new);

            // Run the Hungarian Algorithm on the scoresMatrix
            // If there are less columns that rows, some rows (i.e. query entities) will not be assigned to a column.
            // More specifically they will be assigned to a column id of -1 
            HungarianAlgorithm ha = new HungarianAlgorithm(scoresMatrix);
            int[] assignmentArray = ha.execute();
            List<Integer> assignmentList = Arrays.stream(assignmentArray).boxed().collect(Collectors.toList());

            tupleToColumnMappings.add(assignmentList);
        }
        
        return tupleToColumnMappings;
    }


    /*
     * The similarity between two entities (this is a score between 0 and 1)
     * 
     * By default the similarity is the jaccard similarity of the entity types corresponding to the entities.
     * 
     * However if 'usePretrainedEmbeddings' is specified and there exist embeddings for both entities 
     * then use the angular distance between the two embedding vectors as the score.
     * 
     * If 'usePretrainedEmbeddings' is not specified but 'adjustedJaccardSimilarity' is specified then 
     * an adjusted Jaccard similarity between two entities is used where the similarity score is 1 only if the two entities are identical.
     * Otherwise a maximum similarity score is placed if the two entities are different 

     * 
     * Returns a number between 0 and 1    
     */
    public double entitySimilarityScore(String ent1, String ent2) {

        // Jaccard similarity 
        if (! usePretrainedEmbeddings) {
            Set<String> entTypes1 = new HashSet<>();
            Set<String> entTypes2 = new HashSet<>();
            if (entityTypes.containsKey(ent1)) {
                entTypes1 = new HashSet<String>(entityTypes.get(ent1));
            }
            if (entityTypes.containsKey(ent2)) {
                entTypes2 = new HashSet<String>(entityTypes.get(ent2));
            }
            
            Double jaccard_score = 0.0;
            if (weightedJaccardSimilarity) {
                // Run weighted Jaccard Similarity
                jaccard_score = JaccardSimilarity.make(entTypes1, entTypes2, true).similarity();
            }
            else {
                jaccard_score = JaccardSimilarity.make(entTypes1, entTypes2).similarity();
            }

            if (adjustedJaccardSimilarity) {
                if (ent1.equals(ent2)) {
                    // The two entities compared are equal so return a score of 1.0
                    return 1.0;
                }
                else {
                    // The two entities compared are different. Impose a maximum possible score less than 1.0
                    double max_score = 0.95;
                    // double score = JaccardSimilarity.make(entTypes1, entTypes2).similarity();
                    if (jaccard_score > max_score) {
                        return max_score;
                    }
                    return jaccard_score;
                }
            }
            
            return jaccard_score;
        }

        // Check if the `usePretrainedEmbeddings` mode is specified and if there are embeddings for both entities
        if (usePretrainedEmbeddings && entityExists(ent1) && entityExists(ent2)) {
            
            // Compute the appropriate score based on the specified EmbeddingSimFunction
            String embSimFunction = embeddingSimFunction.getEmbeddingSimFunction();
            Double cosineSim = utils.cosineSimilarity(getEmbeddingVector(ent1), getEmbeddingVector(ent2));
            Double simScore = 0.0;
            if (embSimFunction.equals("norm_cos")) {
                simScore = (cosineSim + 1.0) / 2.0;
            }
            else if (embSimFunction.equals("abs_cos")) {
                simScore = Math.abs(cosineSim);
            }
            else if (embSimFunction.equals("ang_cos")) {
                simScore = 1 - Math.acos(cosineSim) / Math.PI;
            }
            
            numEmbeddingSimComparisons += 1;    // TODO: This must be surrounded by mutex lock
            return simScore;
        }
        else {
            // No mapped pre-trained embeddings found for both `ent1` and `ent2` so we skip this comparison and return 0
            numNonEmbeddingSimComparisons += 1; // TODO: Mutex here as well
            return 0.0;
        }

    }



    /*
     * Compute a table score for each table and return the top-k tables with their respective scores
     * 
     * @param vec_similarity_measure: Must be one of {"cosine", "euclidean"}
     */
    public void getFilenameScores(Integer k, String vec_similarity_measure) {
        System.out.println("Computing scores for each table using " + vec_similarity_measure + " similarity...");
        long startTime = System.nanoTime();
    
        for (String filename : similarityVectorMap.keySet()) {
            // Map each query tupleID to a list of all similarity vectors concerning the current filename 
            Map<Integer, List<List<Double>>> tupleIDToListOfSimVectors = new HashMap<>();

            // Loop over each row and query tuple to populate the `tupleIDToListOfSimVectors` HashMap
            for (Integer rowID : similarityVectorMap.get(filename).keySet()) {
                for (Integer tupleID : similarityVectorMap.get(filename).get(rowID).keySet()) {
                    List<Double> curSimVector = similarityVectorMap.get(filename).get(rowID).get(tupleID);
                    if (!tupleIDToListOfSimVectors.containsKey(tupleID)) {
                        List<List<Double>> ListOfSimVectors = new ArrayList<>();
                        ListOfSimVectors.add(curSimVector);
                        tupleIDToListOfSimVectors.put(tupleID, ListOfSimVectors);
                    }
                    else {
                        tupleIDToListOfSimVectors.get(tupleID).add(curSimVector);
                    }
                }
            }
           
            // Compute the weighted vector (i.e. considers IDF scores of query entities) for each query tuple
            Map<Integer, List<Double>> tupleIDToWeightVector = new HashMap<>();
            for (Integer tupleID=0; tupleID < queryEntities.size(); tupleID++) {
                List<Double> curTupleIDFScores = new ArrayList<>();
                for (Integer i=0; i < queryEntities.get(tupleID).size(); i++) {
                    curTupleIDFScores.add(entityToIDF.get(queryEntities.get(tupleID).get(i)));
                }
                tupleIDToWeightVector.put(tupleID, utils.normalizeVector(curTupleIDFScores));
            }

            // 2D List mapping each tupleID to the similarity scores chosen across the aligned columns
            List<List<Double>> tupleVectors = new ArrayList<>();
            
            // Compute a score for the current file with respect to each query tuple
            // The score takes into account the weight vector associated with each tuple
            Map<Integer, Double> tupleIDToScore = new HashMap<>();
            for (Integer tupleID=0; tupleID < queryEntities.size(); tupleID++) {
                if (similarityVectorMap.get(filename).size() > 0) {
                    // There is at least one data row that has values mapping to known entities

                    // ensure that the current tupleID has at least one similarity vector with some row
                    if (tupleIDToListOfSimVectors.containsKey(tupleID)) {                   
                        List<Double> curTupleVec = new ArrayList<Double>();
                        if (useMaxSimilarityPerColumn) {
                            // Use the maximum similarity score per column as the tuple vector
                            curTupleVec = utils.getMaxPerColumnVector(tupleIDToListOfSimVectors.get(tupleID));
                        }
                        else {
                            curTupleVec = utils.getAverageVector(tupleIDToListOfSimVectors.get(tupleID));
                        }
                        List<Double> identityVector = new ArrayList<Double>(Collections.nCopies(curTupleVec.size(), 1.0));
                        Double score = 0.0;

                        if (vec_similarity_measure == "cosine") {
                            // Note: Cosine similarity doesn't make sense if we are operating in a vector similarity space
                            score = utils.cosineSimilarity(curTupleVec, identityVector);
                        }
                        else if (vec_similarity_measure == "euclidean") {
                            // Perform weighted euclidean distance between the `curTupleVec` and `identityVector` vectors where the weights are specified by `tupleIDToWeightVector.get(tupleID)`
                            score = utils.euclideanDistance(curTupleVec, identityVector, tupleIDToWeightVector.get(tupleID));
                            // Convert euclidean distance to similarity, high similarity (i.e. close to 1) means euclidean distance is small
                            score = 1 / (score + 1);
                        }
                        tupleIDToScore.put(tupleID, score);

                        // Update the tupleVectors array
                        tupleVectors.add(curTupleVec);
                    }
                }
                else {
                   // No entity maps to any value in any row in this table so we give the file a score of zero
                   tupleIDToScore.put(tupleID, 0.0);
                }
            }

            // TODO: Each tuple currently weighted equally. Maybe add extra weighting per tuple when taking average?
            // Get a single score for the current filename that is averaged across all query tuple scores
            if (!tupleIDToScore.isEmpty()) {
                List<Double> tupleIDScores = new ArrayList<Double>(tupleIDToScore.values()); 
                Double fileScore = utils.getAverageOfVector(tupleIDScores);
                filenameToScore.put(filename, fileScore);
                filenameToStatistics.get(filename).put("tupleScores", tupleIDScores);
                filenameToStatistics.get(filename).put("tupleVectors", tupleVectors);
            }
            else {
                filenameToScore.put(filename, 0.0);
                filenameToStatistics.get(filename).put("tupleScores", Arrays.asList(0.0));
                filenameToStatistics.get(filename).put("tupleVectors", Arrays.asList(0.0));
            }
        } 

        System.out.println("Elapsed time: " + (System.nanoTime() - startTime) /(1e9) + " seconds\n");

        // Sort the scores for each file and return the top-k filenames
        filenameToScore = sortByValue(filenameToScore);

        System.out.println("\nTop-" + k + " tables are:");
        Integer i = 0;
        for (Map.Entry<String, Double> en : filenameToScore.entrySet()) {
            System.out.println("filename = " + en.getKey() + ", score = " + en.getValue());
            i += 1;
            if (i > k) {
                break;
            }
        }
    }


    public int ppr() {
        // Initialize the connector
        try {
            this.connector = new Neo4jEndpoint(this.configFile);
            connector.testConnection();
        } catch(AuthenticationException ex){
            System.err.println( "Could not Login to Neo4j Server (user or password do not match)");
            System.err.println(ex.getMessage());
        }catch (ServiceUnavailableException ex){
            System.err.println( "Could not connect to Neo4j Server");
            System.err.println(ex.getMessage());
        } catch (FileNotFoundException ex){
            System.err.println( "Configuration file for Neo4j connector not found");
            System.err.println(ex.getMessage());
        } catch ( IOException ex){
            System.err.println("Error in reading configuration for Neo4j connector");
            System.err.println(ex.getMessage());
        }


        if (pprSingleRequestForAllQueryTuples) {
            queryEntities = ppr.combineQueryTuplesInSingleTuple(queryEntities);
        }

        List<List<Double>> weights;
        if (weightedPPR) {
            // Extract weights for each query tuple
            weights = ppr.getWeights(connector, queryEntities, entityToIDF);
        }
        else {
            weights = ppr.getUniformWeights(queryEntities);
        }

        long startTime = System.nanoTime();
        System.out.println("\n\nRunning PPR over the " + queryEntities.size() + " provided Query Tuple(s)...");
        System.out.println("PPR Weights: " + weights);

        // Run PPR once from each query tuple
        for (Integer i=0; i<queryEntities.size(); i++) {
            Map<String, Double> curTupleFilenameToScore = connector.runPPR(queryEntities.get(i), weights.get(i), minThreshold, numParticles, topK);
            
            // Update the 'filenameToScore' accordingly
            for (String s : curTupleFilenameToScore.keySet()) {
                if (!filenameToScore.containsKey(s)) {
                    filenameToScore.put(s, curTupleFilenameToScore.get(s));
                }
                else {
                    filenameToScore.put(s, filenameToScore.get(s) + curTupleFilenameToScore.get(s));
                }
            }
            System.out.println("Finished computing PPR for tuple: " + i);
        }
        elapsedTime = (System.nanoTime() - startTime) / 1e9;
        System.out.println("\n\nFinished running PPR over the given Query Tuple(s)");    
        System.out.println("Elapsed time: " + elapsedTime + " seconds\n");

        // Sort the scores for each file
        filenameToScore = sortByValue(filenameToScore);

        // Save the PPR scores to a json file
        this.saveFilenameScores(outputDir);

        return 1;
    }



    // function to sort hashmap by values
    public static Map<String, Double> sortByValue(Map<String, Double> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(hm.entrySet());
  
        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Double> >() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                int i = (o1.getValue()).compareTo(o2.getValue());
                if(i != 0) return -i;
                return i;
            }
        });
          
        // put data from sorted list to hashmap 
        Map<String, Double> temp = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    /**
     * Deserialize all hashmaps from a specified directory
     */
    public boolean deserializeHashMaps(File path) {
        System.out.println("Deserializing Hash Maps...");
        
        try {

            FileInputStream fileIn = new FileInputStream(path+"/entityToFilename.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            entityToFilename = (HashMap) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized entityToFilename");

            fileIn = new FileInputStream(path+"/entityToIDF.ser");
            in = new ObjectInputStream(fileIn);
            entityToIDF = (HashMap) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized entityToIDF");

            // If mode is ppr not all items need to be deserialized
            if (!searchMode.getMode().equals("ppr")) {
                fileIn = new FileInputStream(path+"/wikipediaLinkToEntity.ser");
                in = new ObjectInputStream(fileIn);
                wikipediaLinkToEntity = (HashMap) in.readObject();
                in.close();
                fileIn.close();
                System.out.println("Deserialized wikipediaLinkToEntity");

                fileIn = new FileInputStream(path+"/entityTypes.ser");
                in = new ObjectInputStream(fileIn);
                entityTypes = (HashMap) in.readObject();
                in.close();
                fileIn.close();
                System.out.println("Deserialized entityTypes");

                fileIn = new FileInputStream(path+"/entityInFilenameToTableLocations.ser");
                in = new ObjectInputStream(fileIn);
                entityInFilenameToTableLocations = (HashMap) in.readObject();
                in.close();
                fileIn.close();
                System.out.println("Deserialized entityInFilenameToTableLocations");

            }

            if (usePretrainedEmbeddings) {
                Gson gson = new Gson();
                Reader reader = new FileReader(embeddingsPath);
        
                // convert JSON file to a hashmap and then extract the list of queries
                Type type = new TypeToken<HashMap<String, List<Double>>>(){}.getType();
                Map<String, List<Double>> map = gson.fromJson(reader, type);
                entityToEmbedding = map;
                reader.close();

                System.out.println("Read pre-trained embeddings into a HashMap");
            }

            if (weightedJaccardSimilarity) {
                // Read the entityTypeToIDF.ser file
                fileIn = new FileInputStream(path+"/entityTypeToIDF.ser");
                in = new ObjectInputStream(fileIn);
                entityTypeToIDF = (HashMap) in.readObject();
                in.close();
                fileIn.close();
                System.out.println("Deserialized entityTypeToIDF");
            }

            return true;
        } 
        catch (IOException i) {
            i.printStackTrace();
            return false;
        }
        catch (ClassNotFoundException c) {
            System.out.println("Class not found");
            c.printStackTrace();
            return false;
        }
    }

    public List<List<String>> parseQuery(File path) {

        List<List<String>> queryEntities = new ArrayList<>();
        try {
            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(path.toPath());
    
            // convert JSON file to a hashmap and then extract the list of queries
            Type type = new TypeToken<HashMap<String, List<List<String>>>>(){}.getType();
            Map<String, List<List<String>>> map = gson.fromJson(reader, type);
            queryEntities = map.get("queries");

            reader.close();    
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        switch (this.queryMode){
            case TUPLE:
                System.out.println("Query mode: " + queryMode.getMode());
                break;
            case ENTITY:
                System.out.println("Query mode: " + queryMode.getMode());
                System.err.println("Entity Query mode is not supported yet!");
                System.exit(-1);
                break;
        }

        return queryEntities;
    }

    /**
     * Saves the data of the filenameToScore Hashmap into the "filenameToScore.json" file at the specified output directory
     */
    // TODO: Wrap this in RAII for better control
    public synchronized void saveFilenameScores(File outputDir) {

        File saveDir = new File(outputDir, "/search_output/");
        if (!saveDir.exists()){
            saveDir.mkdir();
        }

        System.out.println("\nConstructing the filenameToScore.json file...");

        // Specify the format of the filenameToScore.json file 
        JsonObject jsonObj = new JsonObject();
        JsonArray innerObjs = new JsonArray();
        
        // Iterate over filenameToScore hashmap
        for (String file : ProgressBar.wrap(filenameToScore.keySet(), "Processing files...")) {
            JsonObject tmp = new JsonObject();
            tmp.addProperty("tableID", file);
            tmp.addProperty("score", filenameToScore.get(file));
            
            // Get Page Title and URL of the current file
            JsonTable table = utils.getTableFromPath(Paths.get(this.tableDir.toString()+"/" + file));
            String pgTitle = table.pgTitle;
            String tableURL = "https://en.wikipedia.org/wiki/"+pgTitle.replace(' ', '_');
            tmp.addProperty("pgTitle", pgTitle);
            tmp.addProperty("tableURL", tableURL);

            // Add Statistics for current filename
            if (searchMode.getMode() != "ppr") {
                tmp.addProperty("numEntityMappedRows", filenameToStatistics.get(file).get("numEntityMappedRows").toString());
                tmp.addProperty("fractionOfEntityMappedRows", filenameToStatistics.get(file).get("fractionOfEntityMappedRows").toString());
                tmp.addProperty("tupleScores", filenameToStatistics.get(file).get("tupleScores").toString());
                tmp.addProperty("tupleVectors", filenameToStatistics.get(file).get("tupleVectors").toString());
            }

            if (singleColumnPerQueryEntity) {
                tmp.addProperty("tuple_query_alignment", filenameToStatistics.get(file).get("tuple_query_alignment").toString());
            }

            innerObjs.add(tmp);
        }
        jsonObj.add("scores", innerObjs);

        // Runtime to process all tables (does not consider time to compute scores)
        jsonObj.addProperty("runtime", elapsedTime);

        if (usePretrainedEmbeddings) {
            // Add the embedding statistics
            jsonObj.addProperty("numEmbeddingSimComparisons", numEmbeddingSimComparisons);
            jsonObj.addProperty("numNonEmbeddingSimComparisons", numNonEmbeddingSimComparisons);
            jsonObj.addProperty("fractionOfEntityMappedRows", (double)numEmbeddingSimComparisons / (numEmbeddingSimComparisons + numNonEmbeddingSimComparisons));

            jsonObj.addProperty("numEmbeddingCoverageSuccesses", hasEmbeddingCoverageSuccesses);
            jsonObj.addProperty("numEmbeddingCoverageFails", hasEmbeddingCoverageFails);
            jsonObj.addProperty("embeddingCoverageSuccessRate", (double)hasEmbeddingCoverageSuccesses / (hasEmbeddingCoverageSuccesses + hasEmbeddingCoverageFails));

            JsonArray qEntitiesMissingCoverageArr = new JsonArray();
            for (String qEnt : queryEntitiesMissingCoverage) {
                qEntitiesMissingCoverageArr.add(qEnt);
            }
            jsonObj.add("queryEntitiesMissingCoverage", qEntitiesMissingCoverageArr);
        }

        try {
            Writer writer = new FileWriter(saveDir+ "/filenameToScore.json");
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(jsonObj, writer);
            writer.close();
        }
        catch (IOException i) {
            i.printStackTrace();
        }

        System.out.println("Finished constructing the filenameToScore.json file.");
    }

    /**
     * Returns true if the specified entity exists
     *
     * If the `embeddingsInputMode` is "file" then it checks for existence using the `entityToEmbedding` hashmap
     * If the `embeddingsInputMode` is "database" then it queries the database for existence
     */
    public boolean entityExists(String entity) {
        if (embeddingsInputMode.getEmbeddingsInputMode() == "file") {
            if (entityToEmbedding.containsKey(entity)) {return true;}
            else {return false;}
        }
        else if (embeddingsInputMode.getEmbeddingsInputMode() == "database") {
            try {
                return this.store.select(entity) != null;
            }
            catch (IllegalArgumentException exc) {
                return false;
            }
        }
        return false; 
    }

    /**
     * Returns the embedding vector for the specified entity.
     * 
     * The entity specified must exist! If it isn't then an empty vector is returned
     *
     * If the `embeddingsInputMode` is "file" then extract the embedding vector from the `entityToEmbedding` hashmap
     * If the `embeddingsInputMode` is "database" then query he database to get the embedding vector
     */
    public List<Double> getEmbeddingVector(String entity) {
        if (embeddingsInputMode.getEmbeddingsInputMode() == "file") {
            if (entityToEmbedding.containsKey(entity)) {
                return entityToEmbedding.get(entity);
            }
        }
        else if (embeddingsInputMode.getEmbeddingsInputMode() == "database") {
            try {
                return this.store.select(entity);
            }
            catch (IllegalArgumentException exception) {
                return List.of();
            }
        }
        return List.of();
    }

}
