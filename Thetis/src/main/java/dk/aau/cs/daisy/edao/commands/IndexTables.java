package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;


import dk.aau.cs.daisy.edao.structures.Pair;
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
     * java -jar Thetis.1.0.jar  index --config ./config.properties --type wikitables --tables  data/tables/wikitables --output data/index/wikitables
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
            500000,
            0.01);
    public long indexWikiTables(){

        // Open table directory
        // Iterate each table
        // Extract table + column + row coordinates -> link to wikipedia
        // Query neo4j (in batch??) to get the matches
        // Save information on a file for now

        long parsedTables;
        try {
            parsedTables = Files.find(this.tableDir.toPath(),
                    Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"))
                    .map(filePath -> this.parseTable(filePath.toAbsolutePath())).filter(res -> res).count();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        System.out.printf("Found an approximate total of %d of unique entity mentions%n", this.filter.approximateElementCount() );

        return parsedTables;


    }


    public boolean parseTable(Path path) {
        JsonTable table;
        Gson gson = new GsonBuilder().serializeNulls().create();

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

        if(table == null || table._id  == null || table.body == null) {
            System.err.println("Failed to parse '"+path.toString()+"'");
            try {
                System.err.println(Files.readString(path));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return  false;
        }

        System.out.println("Table: "+ table._id );

        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();
        int rowId = 0;
        for(List<JsonTable.TableCell> row : table.body){
            int collId =0;
            for(JsonTable.TableCell cell : row ){
                if(!cell.links.isEmpty()){
                    //List<Pair<String,String>> matchedUris = connector.searchLinks(cell.links);
                    List<String> matchedUris = connector.searchLinks(cell.links);
                    for(String em : matchedUris){
                        this.filter.put(em);
                    }
                    entityMatches.put(new Pair<>(rowId, collId), matchedUris);
                }
                collId+=1;
            }
            rowId+=1;
        }

        System.out.printf("Found a total of %d entity matches%n", entityMatches.size());

        return true;
    }


}
