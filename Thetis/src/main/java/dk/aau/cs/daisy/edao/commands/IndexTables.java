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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.util.Pair;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;


// import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.tables.JsonTable;

import picocli.CommandLine;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;


import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;


@picocli.CommandLine.Command(name = "index", description = "creates the index for the specified set of tables")
public class IndexTables extends Command {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    /**
     * java -jar Thetis.1.0.jar  index --config ./config.properties --table-type wikitables --table-dir  data/tables/wikitables --output-dir data/index/wikitables
     */
    public enum TableType {
        WIKI("wikitables"),
        TT("toughtables");

        private final String name;
        TableType(String name){
            this.name = name;
        }

        public final String getName(){
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }


    @CommandLine.Option(names = { "-tt", "--table-type" }, description = "Table types: ${COMPLETION-CANDIDATES}", required = true)
    private TableType tableType = null;



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


    private File tableDir = null;
    @CommandLine.Option(names = { "-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing tables", required = true)
    public void setTableDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }
        tableDir = value;
    }


    private File outputDir = null;
    @CommandLine.Option(names = { "-od", "--output-dir" }, paramLabel = "OUT_DIR", description = "Directory where to save the index", required = true)
    public void setOutputDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }

        if (!value.canWrite()) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory is not writable.", value));
        }

        outputDir = value;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////



    private Neo4jEndpoint connector;

    @Override
    public Integer call() {
        System.out.println("IndexTables command run: not fully implemented");

        System.out.println("Input Directory: " + this.tableDir.getAbsolutePath() );
        System.out.println("Output Directory: " + this.outputDir.getAbsolutePath() );

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


        long parsedTables;
        switch (this.tableType){
            case TT:
                System.err.println( "Indexing of '"+TableType.TT.getName() + "' is not supported yet!" );
                break;
            case WIKI:
                System.out.println("Starting index of '"+TableType.WIKI.getName()+"'");
                parsedTables = this.indexWikiTables();
                System.out.printf("Indexed %d tables%n", parsedTables );
                break;

        }

        System.out.println("Done.");

        return 23;
    }


    private final BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            5_000_000,
            0.01);

    // Map a wikipedia uri to a dbpedia uri (e.g. https://en.wikipedia.org/wiki/Yellow_Yeiyah -> http://dbpedia.org/resource/Yellow_Yeiyah)
    private final Map<String, String> wikipediaLinkToEntity = new HashMap<>(100000);

    // Map a dbpedia uri to its list of rdf__types (e.g. http://dbpedia.org/resource/Yellow_Yeiyah -> [http://dbpedia.org/ontology/Swimmer, http://dbpedia.org/ontology/Person,  http://dbpedia.org/ontology/Athlete])
    private final Map<String, List<String>> entityTypes = new HashMap<>();

    // Inverted index that maps each entity uri (i.e. dbpedia uri) to a map of filenames which in turn map to a list of [rowId, colId] pairs twhere the entity is found
    // e.g. entityInvertedIndex.get('http://dbpedia.org/resource/Yellow_Yeiyah') = {'table-316-3.json': [2,10], [3,10], ...}
    private final Map<String, Map<String, List<Pair<Integer, Integer>>>> entityInvertedIndex = new HashMap<>();

    private int cellsWithLinks = 0;


    public long indexWikiTables(){

        // Open table directory
        // Iterate each table (each table is a JSON file)
        // Extract table + column + row coordinates -> link to wikipedia page
        // Query neo4j (in batch??) to get the matches
        // Save information on a file for now

        long parsedTables = 0;
        // Parse all tables in the specified directory
        try {
            // parsedTables = Files.find(this.tableDir.toPath(),
            //         Integer.MAX_VALUE,
            //         (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"))
            //         .map(filePath -> this.parseTable(filePath.toAbsolutePath())).filter(res -> res).count();

            // Get a list of all the files from the specified directory
            Stream<Path> file_stream = Files.find(this.tableDir.toPath(),
                Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> file_paths_list = file_stream.collect(Collectors.toList());
            System.out.println("There are " + file_paths_list.size() + " files to be processed.");

            // Parse each file (TODO: Maybe parallelise this process? How can the global variables be shared?)
            for (int i=0; i < 10; i++) {
                if (this.parseTable(file_paths_list.get(i).toAbsolutePath())) {
                    parsedTables += 1;
                }
                if ((i % 100) == 0) {
                    System.out.println("Processed " + i + "/" + file_paths_list.size() + " files...");
                }
            }
            System.out.println("A total of " + parsedTables + " tables were parsed.");
            
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        // Save the produced hashmaps to disk
        if (this.saveData()) {
            System.out.println("Successfully serialized all hashmaps!");
        }

        System.out.printf("Found an approximate total of %d  unique entity mentions across %d cells %n", this.filter.approximateElementCount(), this.cellsWithLinks );

        return parsedTables;
    }


    /**
     *
     * @param path to a single Json file for a wikipedia table
     * @return true if parsing was successful
     */
    public boolean parseTable(Path path) {


        // Initialization
        JsonTable table;
        Gson gson = new GsonBuilder().serializeNulls().create();

        // Tries to parse the JSON file, it fails if file not found or JSON is not well formatted
        TypeAdapter<JsonTable> strictGsonObjectAdapter =
                new Gson().getAdapter(JsonTable.class);
        try (JsonReader reader = new JsonReader(new FileReader(path.toFile()))) {
            table = strictGsonObjectAdapter.read(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }


        // We check if all the required json attributes are set
        if(table == null || table._id  == null || table.rows == null) {
            System.err.println("Failed to parse '"+path.toString()+"'");
            try {
                System.err.println(Files.readString(path));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return  false;
        }

        // System.out.println("Table: "+ table._id );

        String filename = path.getFileName().toString();

        // Maps RowNumber, ColumnNumber -> Wikipedia links contained in it
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();

        int rowId = 0;
        // Loop over every cell in a table
        for(List<JsonTable.TableCell> row : table.rows){
            int collId =0;
            for(JsonTable.TableCell cell : row ){
                if(!cell.links.isEmpty()) {
                    cellsWithLinks+=1;
                    //List<Pair<String,String>> matchedUris = connector.searchLinks(cell.links);
                    List<String> matchedUris = new ArrayList<>();
                    for(String link : cell.links){
                        if(wikipediaLinkToEntity.containsKey(link)){ //Check if we had already searched for it
                            matchedUris.add(wikipediaLinkToEntity.get(link));
                        } 
                        else { 
                            // Query the Neo4j DB to find the entitity corresponding to a wikilink from a cell value
                            List<String> tempLinks = connector.searchLink(link.replace("http://www.", "http://en."));
                            if(!tempLinks.isEmpty()) {
                                // Currently only select a single dbpedia entity (i.e. the first one) for each wikilink
                                String entity = tempLinks.get(0);
                                matchedUris.add(entity);
                                wikipediaLinkToEntity.put(link, entity);

                                // Retrieve a list of rdf__types of the entity and save them, in entityTypes map
                                List<String> entity_types_uris = connector.searchTypes(entity);
                                entityTypes.put(entity, entity_types_uris); 
                            }
                        }

                        // Each wikilink is mapped to an entity (i.e. a dbpedia entry) in wikipediaLinkToEntity so we can update the entityInvertedIndex for each cell we visit
                        if(wikipediaLinkToEntity.containsKey(link)) {
                            String entity = wikipediaLinkToEntity.get(link);
                            Pair<Integer, Integer> cur_pair = new Pair<>(rowId, collId);

                            if (entityInvertedIndex.containsKey(entity)) {
                                // Check if `filename` is a key in the hashmap returned from entityInvertedIndex.get(entity)
                                if (entityInvertedIndex.get(entity).containsKey(filename)) {
                                    // Append the cur_pair in the existing list
                                    entityInvertedIndex.get(entity).get(filename).add(cur_pair);
                                }
                                else {
                                    // `filename` not a key in the hashmap so add it and assign it to a new list that includes the cur_pair
                                    List<Pair<Integer, Integer>> list_of_cell_locs = new ArrayList<>();
                                    list_of_cell_locs.add(cur_pair);
                                    entityInvertedIndex.get(entity).put(filename, list_of_cell_locs);
                                }
                            }
                            else {
                                // First time entity is a key in the entityInvertedIndex
                                List<Pair<Integer, Integer>> list_of_cell_locs = new ArrayList<>();
                                list_of_cell_locs.add(cur_pair);
                                entityInvertedIndex.put(entity, new HashMap(){{put(filename, list_of_cell_locs);}});
                            }
                        }

                    }
                    if (!matchedUris.isEmpty()) {
                        for (String em : matchedUris) {
                            this.filter.put(em);
                        }

                        entityMatches.put(new Pair<>(rowId, collId), matchedUris);

                        /// We need also the inverse mapping, from entity to the table + cells
                    }
                    
                    // System.out.println("Cell: " + cell.text + " matches to the following uris:");
                    // for(String uri : matchedUris) {
                    //     System.out.println(uri);
                    // }
                    // System.out.print("\n");
                }

                collId+=1;
            }
            rowId+=1;
        }

        //System.out.printf("Found a total of %d entity matches%n", entityMatches.size());

        return true;
    }


    /**
     * Seriealize data
     */
    public boolean saveData() {
        // Serialize the hash maps
        try {
            FileOutputStream fileOut = new FileOutputStream("/wikipediaLinkToEntity.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(wikipediaLinkToEntity);
            out.close();
            fileOut.close();
            System.out.println("Serialized wikipediaLinkToEntity hashmap");

            fileOut = new FileOutputStream("/entityTypes.ser");
            out = new ObjectOutputStream(fileOut);
            out.writeObject(entityTypes);
            out.close();
            fileOut.close();
            System.out.println("Serialized entityTypes hashmap");

            fileOut = new FileOutputStream("/entityInvertedIndex.ser");
            out = new ObjectOutputStream(fileOut);
            out.writeObject(entityInvertedIndex);
            out.close();
            fileOut.close();
            System.out.println("Serialized entityInvertedIndex hashmap");

            return true;
        } catch (IOException i) {
            i.printStackTrace();
            return false;
        }
        
        // //De-serialization Code:

        // try {
        //     FileInputStream fileIn = new FileInputStream("/wikipediaLinkToEntity.ser");
        //     ObjectInputStream in = new ObjectInputStream(fileIn);
        //     Map<String, String> test = new HashMap<>();
        //     test = (HashMap) in.readObject();
        //     in.close();
        //     fileIn.close();

        //     System.out.println("Loading serealized data");
        //     for (String key : test.keySet()) {
        //         String val = test.get(key);
        //         System.out.println(key + " : " + val);
        //     }
        // } catch (IOException i) {
        //     i.printStackTrace();
        //     return -1;
        // }
        // catch(ClassNotFoundException c) {
        //     System.out.println("Class not found");
        //     c.printStackTrace();
        //     return -1;
        // }
    }
}

