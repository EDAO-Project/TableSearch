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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;

import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.utils;
import dk.aau.cs.daisy.edao.utilities.HungarianAlgorithm;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;

import picocli.CommandLine;

@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {

    //********************* Command Line Arguements *********************//
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

    @CommandLine.Option(names = { "-sm", "--search-mode" }, description = "Must be one of {exact, analogous}", required = true)
    private SearchMode searchMode = null;

    @CommandLine.Option(names = { "-qm", "--query-mode" }, description = "Must be one of {tuple, entity}", required = true, defaultValue = "tuple")
    private QueryMode queryMode = null;

    @CommandLine.Option(names = { "-scpqe", "--singleColumnPerQueryEntity"}, description = "If specified, each query tuple will be evaluated against only one entity")
    private boolean singleColumnPerQueryEntity; 

    private File hashmapDir = null;
    @CommandLine.Option(names = { "-hd", "--hashmap-dir" }, paramLabel = "HASH_DIR", description = "Directory from which we load the hashmaps", defaultValue = "../data/index/wikitables/")
    public void setHashMapDirectory(File value) {
        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--hashmap-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--hashmap-dir': " + "the path does not point to a directory.", value));
        }

        hashmapDir = value;
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

    private Neo4jEndpoint connector;

    @Override
    public Integer call() {
        System.out.println("Hashmap Directory: " + hashmapDir);
        System.out.println("Query File: " + queryFile);
        System.out.println("Table Directory: " + tableDir);
        System.out.println("Output Directory: " + outputDir);
        System.out.println("Single Column per Query Entity: " + singleColumnPerQueryEntity);

        // Create output directory if it doesn't exist
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Read off the queryEntities list from a json object
        queryEntities = this.parseQuery(queryFile);
        System.out.println("Query Entities: " + queryEntities + "\n");

        if (searchMode.getMode() != "ppr") {
            // Perform De-Serialization of the indices
            long startTime = System.nanoTime();    
            if (this.deserializeHashMaps(hashmapDir)) {
                System.out.println("Deserialization successful!\n");
                System.out.println("Elapsed time for deserialization: " + (System.nanoTime() - startTime)/(1e9) + " seconds\n");
            }
            else {
                System.out.println("de-serialization Failed!\n");
                return -1;
            }

            // Ensure that all queryEntities are searchable over the index
            System.out.println("\nIDF scores for each entity:");
            for (Integer i=0; i<queryEntities.size(); i++) {
                System.out.println("Query Tuple: " + i);
                for (String entity : queryEntities.get(i)) {
                    if (!entityToFilename.containsKey(entity)) {
                        System.out.println(entity + " does not map to any known entity in the constructed index");
                        return -1;
                    }
                    else {
                        System.out.println(entity + ": " + entityToIDF.get(entity));
                    }
                }
                System.out.println("\n");
            }
        } 

        // Perform search according to the `search-mode`
        switch (this.searchMode){
            case EXACT:
                System.out.println("Search mode: " + searchMode.getMode());
                this.exactSearch();
                break;
            case ANALOGOUS:
                System.out.println("Search mode: " + searchMode.getMode());
                this.analogousSearch();
                break;
            case PPR:
                System.out.println("Search mode: " + searchMode.getMode());
                this.ppr();
                break;
        }

        return 1;
    }

    //********************* Global Variables *********************//

    // A doubly nested list of strings containing the entities for each tuple. If the query mode is entities the array is still doubly nested but there is only one row with the list of entities.
    private List<List<String>> queryEntities = new ArrayList<>();

    // Map a wikipedia uri to a dbpedia uri (e.g. https://en.wikipedia.org/wiki/Yellow_Yeiyah -> http://dbpedia.org/resource/Yellow_Yeiyah)
    private Map<String, String> wikipediaLinkToEntity = new HashMap<>(100000);

    // Map a dbpedia uri to its list of rdf__types (e.g. http://dbpedia.org/resource/Yellow_Yeiyah -> [http://dbpedia.org/ontology/Swimmer, http://dbpedia.org/ontology/Person,  http://dbpedia.org/ontology/Athlete])
    private Map<String, List<String>> entityTypes = new HashMap<>();

    // Map an entity (i.e. dbpedia uri) to the list of filenames it is found.  
    // e.g. entityToFilename.get("http://dbpedia.org/resource/Yellow_Yeiyah") = [table-316-3.json, ...]
    private Map<String, List<String>> entityToFilename = new HashMap<>();

    // Map an entity in a filename (the key is the entity_URI + "__" + filename) to the list of [rowId, colId] where the entity is found
    // e.g. entityInFilenameToTableLocations.get("http://dbpedia.org/resource/Yellow_Yeiyah__table-316-3.json") = [[0,2], [2,2], ...]
    private Map<String, List<List<Integer>>> entityInFilenameToTableLocations = new HashMap<>();

    // A triple nested map corresponding to a tablename, rowNumber and query tuple number and mapping to its maximal similarity vector 
    private Map<String, Map<Integer, Map<Integer, List<Double>>>> similarityVectorMap = new HashMap<>();

    // Maps each filename to its relevance score according to the query
    private Map<String, Double> filenameToScore = new HashMap<>();

    // Maps each entity to its IDF score. The idf score of an entity is given by log(N/(1+n_t)) + 1 where N is the number of filenames/tables in the repository
    // and n_t is the number of tables that contain the entity in question.
    private Map<String, Double> entityToIDF = new HashMap<>();

    //********************* Global Variables of Statistics *********************//

    private Map<String, Map<String, Object>> filenameToStatistics = new HashMap<>();

    public void exactSearch() {

        for (Integer i=0; i<queryEntities.size(); i++) {
            for (String entity : queryEntities.get(i)) {
                if (entityToFilename.containsKey(entity)) {
                    System.out.println("There are " + entityToFilename.get(entity).size() + " files that contain the entity: " + entity);
                }
                else {
                    System.out.println(entity + " does not map to any known entity in the constructed index.");
                }
            }
        }

        // Currently treat queryEntities as a set of entities not tuples
        // TODO: Allow for tuple query functionality

        List<String> queryEntitiesFlat = queryEntities.stream().flatMap(Collection::stream).collect(Collectors.toList());

        // Find exact tuple matches from the query
        this.exactTupleMatches(queryEntitiesFlat);

        // Analyze all pairwise combinations of query entities
        System.out.println("\n2-entities analysis:");
        for(int i = 0 ; i < queryEntitiesFlat.size(); i++){
            for(int j = i+1 ; j < queryEntitiesFlat.size(); j++){
                List<String> queryEntitiesPair = new ArrayList<>();
                queryEntitiesPair.add(queryEntitiesFlat.get(i));
                queryEntitiesPair.add(queryEntitiesFlat.get(j));
                System.out.println("\nPair: " + queryEntitiesPair);
                this.exactTupleMatches(queryEntitiesPair);    
            }
        }
    }

    public void exactTupleMatches(List<String> queryEntities) {
        // Find the set of files that are found in all queryEntities

        List<Set<String>> filenameSetsList = new ArrayList<>();

        for (String entity : queryEntities) {
            filenameSetsList.add(new HashSet<String>(entityToFilename.get(entity)));
        }

        // Initialize the filename intersection set to the first element in the filenameSetsList
        Set<String> sharedFilenameSet = new HashSet<String>();
        sharedFilenameSet = filenameSetsList.get(0);
        for (Integer i=1; i < filenameSetsList.size(); i++) {
            sharedFilenameSet.retainAll(filenameSetsList.get(i));
        }
        System.out.println("Query Entities share the following " + sharedFilenameSet.size() + " filenames:" + sharedFilenameSet);

        // Find the number of tuples with all entitites for each filename
        for (String filename : sharedFilenameSet) {
            List<Set<Integer>> rowIdsSetList = new ArrayList<>();
            // Populate the rowIdsList for each queryEntity
            for (String entity : queryEntities) {
                List<List<Integer>> coordinates =  entityInFilenameToTableLocations.get(entity + "__" + filename);
                List<Integer> rowIdsList = new ArrayList<>();
                for (List<Integer> coordinate : coordinates) {
                    rowIdsList.add(coordinate.get(0));
                }
                rowIdsSetList.add(new HashSet<Integer>(rowIdsList));
            }

            // Find the set of rowIds that are common for all queryEntities
            Set<Integer> sharedRowIdsSet = new HashSet<Integer>();
            sharedRowIdsSet = rowIdsSetList.get(0);
            for (Integer i=1; i < rowIdsSetList.size(); i++) {
                sharedRowIdsSet.retainAll(rowIdsSetList.get(i));
            }
            if (sharedRowIdsSet.size() > 0) {
                System.out.println("For filename: " + filename + " there are " + sharedRowIdsSet.size() + " matching tuples");
            }
        }
    }

    /**
     * Given a list of entities, return a ranked list of table candidates
     */
    public int analogousSearch() {

        // Loop over each table in the tables directory
        long parsedTables = 0;
        try {
            // Get a list of all the files from the specified directory
            Stream<Path> file_stream = Files.find(this.tableDir.toPath(), Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> file_paths_list = file_stream.collect(Collectors.toList());
            System.out.println("There are " + file_paths_list.size() + " files to be processed.");

            long startTime = System.nanoTime();    
            // Parse each file (TODO: Maybe parallelise this process? How can the global variables be shared?)
            for (int i=0; i < file_paths_list.size(); i++) {
                if (this.searchTable(file_paths_list.get(i).toAbsolutePath())) {
                    parsedTables += 1;
                }
                if ((i % 100) == 0) {
                    System.out.println("Processed " + i + "/" + file_paths_list.size() + " files...");
                }
            }
            System.out.println("A total of " + parsedTables + " tables were parsed.");
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Elapsed time: " + elapsedTime/(1e9) + " seconds\n");    
        } 
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        System.out.println("Successfully completed search over all tables!\n");

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

        // If each query entity needs to map to only one column find the best mapping
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();
        if (singleColumnPerQueryEntity) {
            tupleToColumnMappings = getQueryToColumnMapping(table);
        }

        String filename = path.getFileName().toString();

        // Map each row number of the table each query tuple to its respective similarity vector
        Map<Integer, Map<Integer, List<Double>>> rowTupleIDVectorMap = new HashMap<>();

        Map<String, Object> statisticsMap = new HashMap<>();
        Integer numEntityMappedRows = 0;    // Number of rows in a table that have at least one cell mapping ot a known entity

        // Loop over each query entity. TODO: Treat this on a tuple by tuple basis
        int rowId = 0;
        // Loop over every cell in a table
        for(List<JsonTable.TableCell> row : table.rows){
            // List<String> rowEntities = new ArrayList<>();
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
                            // rowEntities.add(wikipediaLinkToEntity.get(link));
                            break;
                        }
                    }
                }
            }
            // for(JsonTable.TableCell cell : row){
            //     if(!cell.links.isEmpty()) {
            //         // A cell value may map to multiple entities. Currently use the first one
            //         // TODO: Consider all of them?
            //         for(String link : cell.links) {
            //             // Only consider links for which we have a known entity mapping
            //             if (wikipediaLinkToEntity.containsKey(link)) {
            //                 rowEntities.add(wikipediaLinkToEntity.get(link));
            //                 break;
            //             }
            //         }
            //     }
            //     colId+=1;
            // }

            
            // Compute similarity vectors only for rows that map to at least one entity
            if (!colIdToEntity.isEmpty()) {
                numEntityMappedRows += 1;
                Map<Integer, List<Double>> tupleIDVectorMap = new HashMap<>();

                // For each row and for each query tuple compute the maximal similarity vector
                for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
                    // Initialize the maximum vector for the current tuple, to a zero vector of size equal to the query tuple size.
                    List<Double> maximumTupleVector = new ArrayList<Double>(Collections.nCopies(queryEntities.get(tupleID).size(), 0.0));

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
     * Map each query entity from each query tuple to its best matching column id for the input 'table'   
     */
    public List<List<Integer>> getQueryToColumnMapping(JsonTable table) {

        // Initialize multi-dimensional array indexed by (tupleID, entityID, columnID) mapping to the 
        // aggregated score for that query entity with respect the column
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
        // Indexed by (tupleID, entityID) mapping to the columnID. If a columnID is -1 then that entity is was not chosen for assignment
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
     * The similarity between two entities is the jaccard similarity of the entity types corresponding to the entities   
     */
    public double entitySimilarityScore(String ent1, String ent2) {
        Set<String> entTypes1 = new HashSet<>();
        Set<String> entTypes2 = new HashSet<>();
        if (entityTypes.containsKey(ent1)) {
            entTypes1 = new HashSet<String>(entityTypes.get(ent1));
        }
        if (entityTypes.containsKey(ent2)) {
            entTypes2 = new HashSet<String>(entityTypes.get(ent2));
        }

        // Compute the Jaccard Similarity
        Set<String> intersection = new HashSet<String>(entTypes1);
        intersection.retainAll(entTypes2);

        Set<String> union = new HashSet<String>(entTypes1);
        union.addAll(entTypes2);

        double jaccardScore = intersection.size() / union.size();

        return jaccardScore;
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
            
            // Compute a score for the current file with respect to each query tuple
            // The score takes into account the weight vector associated with each tuple
            Map<Integer, Double> tupleIDToScore = new HashMap<>();
            for (Integer tupleID=0; tupleID < queryEntities.size(); tupleID++) {
                if (similarityVectorMap.get(filename).size() > 0) {
                    // There is at least one data row that has values mapping to known entities
                    List<Double> curTupleAvgVec = utils.hadamardProduct(utils.getAverageVector(tupleIDToListOfSimVectors.get(tupleID)), tupleIDToWeightVector.get(tupleID));
                    List<Double> identityVector = new ArrayList<Double>(Collections.nCopies(curTupleAvgVec.size(), 1.0));
                    identityVector = utils.hadamardProduct(identityVector, tupleIDToWeightVector.get(tupleID));
                    Double score = 0.0;

                    if (vec_similarity_measure == "cosine") {
                        // Note: Cosine similarity doesn't make sense if we are operating in a vector similarity space
                        score = utils.cosineSimilarity(curTupleAvgVec, identityVector);
                    }
                    else if (vec_similarity_measure == "euclidean") {
                        score = utils.euclideanDistance(curTupleAvgVec, identityVector);
                        // Convert euclidean distance to similarity, high similarity (i.e. close to 1) means euclidean distance is small
                        score = 1 / (score + 1);
                    }
                    tupleIDToScore.put(tupleID, score);
                }
                else {
                   // No entity maps to any value in any row in this table so we give the file a score of zero
                   tupleIDToScore.put(tupleID, 0.0);
                }
            }

            // TODO: Each tuple is weighted equality. Maybe add extra weighting per tuple when taking average?
            // Get a single score for the current filename that is averaged across all query tuple scores
            List<Double> tupleIDScores = new ArrayList<Double>(tupleIDToScore.values()); 
            Double fileScore = utils.getAverageOfVector(tupleIDScores);
            filenameToScore.put(filename, fileScore);

            // Update Statistics
            filenameToStatistics.get(filename).put("tupleScores", tupleIDScores);
        } 

        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Elapsed time: " + elapsedTime/(1e9) + " seconds\n");

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

        long startTime = System.nanoTime();
        System.out.println("\n\nRunning PPR on given Query Tuple(s)...");

        // Currently the score of a file is the sum of the ppr scores for that file across all query tuples
        // TODO: Potentially a better way to normalize? 

        // Run PPR once from each query tuple
        for (Integer i=0; i<queryEntities.size(); i++) {
            // TODO: Maybe run the runPPR() function in parallel for greater efficiency
            Map<String, Double> curTupleFilenameToScore = connector.runPPR(queryEntities.get(i));
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
        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("\n\nFinished running PPR on given Query Tuple(s)");    
        System.out.println("Elapsed time: " + elapsedTime/(1e9) + " seconds\n");

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
            FileInputStream fileIn = new FileInputStream(path+"/wikipediaLinkToEntity.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
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

            fileIn = new FileInputStream(path+"/entityToFilename.ser");
            in = new ObjectInputStream(fileIn);
            entityToFilename = (HashMap) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized entityToFilename");

            fileIn = new FileInputStream(path+"/entityInFilenameToTableLocations.ser");
            in = new ObjectInputStream(fileIn);
            entityInFilenameToTableLocations = (HashMap) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized entityInFilenameToTableLocations");

            fileIn = new FileInputStream(path+"/entityToIDF.ser");
            in = new ObjectInputStream(fileIn);
            entityToIDF = (HashMap) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized entityToIDF");



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
    public void saveFilenameScores(File outputDir) {

        File saveDir = new File(outputDir, "/search_output/");
        if (!saveDir.exists()){
            saveDir.mkdir();
        }

        System.out.println("\nConstructing the filenameToScore.json file...");

        // Specify the formate of the filenameToScore.json file 
        JsonObject jsonObj = new JsonObject();
        JsonArray innerObjs = new JsonArray();
        // Iterate over filenameToScore hashmap
        for (String file : filenameToScore.keySet()) {
            JsonObject tmp = new JsonObject();
            tmp.addProperty("tableID", file);
            tmp.addProperty("score", filenameToScore.get(file));
            
            // Get Page Title and URL of the current file
            JsonTable table = utils.getTableFromPath(Paths.get(this.tableDir.toString()+"/" + file));
            String pgTitle = table.pgTitle;
            String tableURL = "https://en.wikipedia.org/wiki/"+pgTitle.replace(' ', '_');;
            tmp.addProperty("pgTitle", pgTitle);
            tmp.addProperty("tableURL", tableURL);

            // Add Statistics for current filename
            if (searchMode.getMode() != "ppr") {
                tmp.addProperty("numEntityMappedRows", filenameToStatistics.get(file).get("numEntityMappedRows").toString());
                tmp.addProperty("fractionOfEntityMappedRows", filenameToStatistics.get(file).get("fractionOfEntityMappedRows").toString());
                tmp.addProperty("tupleScores", filenameToStatistics.get(file).get("tupleScores").toString());
            }

            innerObjs.add(tmp);
        }
        jsonObj.add("scores", innerObjs);

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


}
