package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.io.FileReader;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;

import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.utils;

import picocli.CommandLine;

@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {

    //********************* Command Line Arguements *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli
    
    public enum SearchMode {
        EXACT("exact"), ANALOGOUS("analogous");

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

    private File hashmapDir = null;
    @CommandLine.Option(names = { "-hd", "--hashmap-dir" }, paramLabel = "HASH_DIR", description = "Directory from which we load the hashmaps", required = true)
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


    @Override
    public Integer call() {
        System.out.println("Hashmap Directory: " + hashmapDir);
        System.out.println("Query File: " + queryFile);
        System.out.println("Table Directory: " + tableDir);
        System.out.println("Output Directory: " + outputDir);

        // Read off the queryEntities list from a json object
        // TODO: Allow the query to be a set of tuples each with a list of entities
        queryEntities = this.parseQuery(queryFile);
        System.out.println("Query Entities: " + queryEntities + "\n");

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
        for (Integer i=0; i<queryEntities.size(); i++) {
            for (String entity : queryEntities.get(i)) {
                if (!entityToFilename.containsKey(entity)) {
                    System.out.println(entity + " does not map to any known entity in the constructed index");
                    return -1;
                }
            }
        }


        // for (String ent : entityToFilename.keySet()) {
        //     System.out.println(ent + " : " + entityToFilename.get(ent));
        // }

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

    // Map an entity in a filename (the key is the entity_URI + "__" + filename) to the list of [rowId, colID] where the entity is found
    // e.g. entityInFilenameToTableLocations.get("http://dbpedia.org/resource/Yellow_Yeiyah__table-316-3.json") = [[0,2], [2,2], ...]
    private Map<String, List<List<Integer>>> entityInFilenameToTableLocations = new HashMap<>();

    // A triple nested map corresponding to a tablename, rowNumber and query tuple number and mapping to its maximal similarity vector 
    private Map<String, Map<Integer, Map<Integer, List<Double>>>> similarityVectorMap = new HashMap<>();

    // Maps each filename to its relevance score according to the query
    private Map<String, Double> filenameToScore = new HashMap<>();

    // Maps each entity to its IDF score. The idf score of an entity is given by log(N/(1+n_t)) + 1 where N is the number of filenames/tables in the repository
    // and n_t is the number of tables that contain the entity in question.
    private Map<String, Double> entityToIDF = new HashMap<>();

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
     */
    public boolean searchTable(Path path) {
        JsonTable table = utils.getTableFromPath(path);
        // Check if the table is empty/erroneous
        if (table.numDataRows == 0) {
            return false;
        }

        // System.out.println("Table: "+ table._id );
        String filename = path.getFileName().toString();

        // Map each row number of the table each query tuple to its respective similarity vector
        Map<Integer, Map<Integer, List<Double>>> rowTupleIDVectorMap = new HashMap<>();

        // Loop over each query entity. TODO: Treat this on a tuple by tuple basis
        int rowId = 0;
        // Loop over every cell in a table
        for(List<JsonTable.TableCell> row : table.rows){
            int collId = 0;
            List<String> rowEntities = new ArrayList<>();
            for(JsonTable.TableCell cell : row){
                if(!cell.links.isEmpty()) {
                    // A cell value may map to multiple entities. Currently use the first one
                    // TODO: Consider all of them?
                    for(String link : cell.links) {
                        // Only consider links for which we have a known entity mapping
                        if (wikipediaLinkToEntity.containsKey(link)) {
                            rowEntities.add(wikipediaLinkToEntity.get(link));
                            break;
                        }
                    }
                }
                collId+=1;
            }

            Map<Integer, List<Double>> tupleIDVectorMap = new HashMap<>(); 

            // For each row and for each query tuple compute the maximal similarity vector
            for (Integer tupleID=0; tupleID<queryEntities.size(); tupleID++) {
                // Initialize the maximum vector for the current tuple, to a zero vector of size equal to the query tuple size.
                List<Double> maximumTupleVector = new ArrayList<Double>(Collections.nCopies(queryEntities.get(tupleID).size(), 0.0));

                for (Integer queryEntityID=0; queryEntityID<queryEntities.get(tupleID).size(); queryEntityID++) {
                    String queryEntity = queryEntities.get(tupleID).get(queryEntityID);
                    Double bestSimScore = 0.0;
                    for (String rowEntity : rowEntities) {
                        // Compute pairwise entity similarity between 'queryEntity' and 'rowEntity'
                        Double simScore = this.entitySimilarityScore(queryEntity, rowEntity);
                        if (simScore > bestSimScore) {
                            bestSimScore = simScore;
                        }
                    }
                    maximumTupleVector.set(queryEntityID, bestSimScore);
                }
                tupleIDVectorMap.put(tupleID, maximumTupleVector);
            }
            rowTupleIDVectorMap.put(rowId, tupleIDVectorMap);
            rowId+=1;
        }

        similarityVectorMap.put(filename, rowTupleIDVectorMap);
        return true;
    }

    /*
     * The simialrity between two entities is the jaccard similarity of the entity types corresponding to the entities   
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
            // List of all similarity vectors concerning the current filename 
            List<List<Double>> vectorList = new ArrayList<>();

            for (Integer rowID : similarityVectorMap.get(filename).keySet()) {
                for (Integer tupleID : similarityVectorMap.get(filename).get(rowID).keySet()) {
                    // TODO: Add custom tuple weighting. Some tuples may be prefered over others
                    vectorList.add(similarityVectorMap.get(filename).get(rowID).get(tupleID));
                }
            }
            // Compute the filescore by comparing the avgVector with the ideal identity vector
            List<Double> avgVector = utils.getVectorAverage(vectorList);
            List<Double> identityVector = new ArrayList<Double>(Collections.nCopies(avgVector.size(), 1.0));
            Double fileScore = 0.0;
            if (vec_similarity_measure == "cosine") {
                fileScore = utils.cosineSimilarity(avgVector, identityVector);
            }
            else if (vec_similarity_measure == "euclidean") {
                fileScore = utils.euclideanDistance(avgVector, identityVector);
                // Convert euclidean distance to similarity, high similarity (i.e. close to 1) means euclidean distance is small
                fileScore = 1 / (fileScore + 1);
            }
            filenameToScore.put(filename, fileScore);
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
    
            // convert JSON file to array of entities
            queryEntities = gson.fromJson(reader, List.class);
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

    public void saveFilenameScores(File outputDir) {
        // File outputDir = new File(path+"/statistics/");
        if (!outputDir.exists()){
            outputDir.mkdir();
        }

        try {
            Writer writer = new FileWriter(outputDir+"/filenameToScore.json");
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(filenameToScore, writer);
            writer.close();
        }
        catch (IOException i) {
            i.printStackTrace();
        }
    }


}
