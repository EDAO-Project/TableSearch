package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.io.FileReader;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.*;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;

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


    @CommandLine.Option(names = { "-sm", "--search-mode" }, description = "Must be one of {exact, analogous}", required = true)
    private SearchMode searchMode = null;

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


    @Override
    public Integer call() {
        System.out.println("Hashmap Directory: " + hashmapDir);
        System.out.println("Query File: " + queryFile);

        // Read off the queryEntities list from a json object
        // TODO: Allow the query to be a set of tuples each with a list of entities
        List<String> queryEntities = this.parseQuery(queryFile);
        System.out.println("Query Entities: " + queryEntities);

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
        for (String entity : queryEntities) {
            if (!entityToFilename.containsKey(entity)) {
                System.out.println(entity + " does not map to any known entity in the constructed index");
                return -1;
            }
        }

        // for (String ent : entityToFilename.keySet()) {
        //     System.out.println(ent + " : " + entityToFilename.get(ent));
        // }

        // Perform search according to the `search-mode`
        switch (this.searchMode){
            case EXACT:
                System.out.println("Search mode: " + searchMode.EXACT.getMode());
                this.exactSearch(queryEntities);
                break;
            case ANALOGOUS:
                System.out.println("Search mode: " + searchMode.ANALOGOUS.getMode());
                System.err.println("Analogous search mode not yet implemented!");
                System.exit(-1);
                break;
        }

        return 1;
    }

    //********************* Global Variables *********************//

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

    public void exactSearch(List<String> queryEntities) {

        for (String entity : queryEntities) {
            if (entityToFilename.containsKey(entity)) {
                System.out.println("There are " + entityToFilename.get(entity).size() + " files that contain the entity: " + entity);
            }
            else {
                System.out.println(entity + " does not map to any known entity in the constructed index.");
            }
        }

        // Find exact tuple matches from the query
        this.exactTupleMatches(queryEntities);

        // Analyze all pairwise combinations of query entities
        System.out.println("\n2-entities analysis:");
        for(int i = 0 ; i < queryEntities.size(); i++){
            for(int j = i+1 ; j < queryEntities.size(); j++){
                List<String> queryEntitiesPair = new ArrayList<>();
                queryEntitiesPair.add(queryEntities.get(i));
                queryEntitiesPair.add(queryEntities.get(j));
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

    public void analogousSearch(List<String> queryEntities) {
        
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

    public List<String> parseQuery(File path) {

        List<String> queryEntities = new ArrayList<>();
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

        return queryEntities;
    }




}
