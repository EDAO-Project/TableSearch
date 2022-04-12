package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dk.aau.cs.daisy.edao.loader.IndexWriter;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.graph.Type;

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

    @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of threads", required = false, defaultValue = "1")
    private int threads;

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
    @CommandLine.Option(names = { "-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing the input tables", required = true)
    public void setTableDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("InvaRoyaltylid value '%s' for option '--table-dir': " +
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
    @CommandLine.Option(names = { "-od", "--output-dir" }, paramLabel = "OUT_DIR", description = "Directory where the index and it metadata are saved", required = true)
    public void setOutputDirectory(File value) {

        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--output-dir': " +
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

    @Override
    public Integer call() {
        long parsedTables;
        System.out.println("Input Directory: " + this.tableDir.getAbsolutePath() );
        System.out.println("Output Directory: " + this.outputDir.getAbsolutePath() );

        try {
            Neo4jEndpoint connector = new Neo4jEndpoint(this.configFile);
            connector.testConnection();

            switch (this.tableType) {
                case TT:
                    System.err.println( "Indexing of '"+TableType.TT.getName() + "' is not supported yet!" );
                    break;
                case WIKI:
                    System.out.println("Starting indexing of '"+TableType.WIKI.getName()+"'");
                    parsedTables = this.indexWikiTables(this.tableDir.toPath(), this.outputDir, connector, this.threads);
                    System.out.printf("Indexed %d tables%n", parsedTables);
                    break;
            }
        } catch(AuthenticationException ex){
            System.err.println("Could not Login to Neo4j Server (user or password do not match)");
            System.err.println(ex.getMessage());
        }catch (ServiceUnavailableException ex){
            System.err.println("Could not connect to Neo4j Server");
            System.err.println(ex.getMessage());
        } catch (FileNotFoundException ex){
            System.err.println("Configuration file for Neo4j connector not found");
            System.err.println(ex.getMessage());
        } catch ( IOException ex){
            System.err.println("Error in reading configuration for Neo4j connector");
            System.err.println(ex.getMessage());
        }

        System.out.println("\n\nDONE\n\n");

        return 23;
    }

    /**
     * Returns number of tables successfully loaded
     * @param tableDir Path to directory of tables to be loaded
     * @param connector Neo4J connector instance
     * @return Number of successfully loaded tables
     */
    public long indexWikiTables(Path tableDir, File outputDir, Neo4jEndpoint connector, int threads){

        // Open table directory
        // Iterate each table (each table is a JSON file)
        // Extract table + column + row coordinates -> link to wikipedia page
        // Query neo4j (in batch??) to get the matches
        // Save information on a file for now

        try
        {
            Stream<Path> fileStream = Files.find(tableDir,
                    Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> filePaths = fileStream.collect(Collectors.toList());
            Collections.sort(filePaths);
            System.out.println("\nThere are " + filePaths.size() + " files to be processed.");

            long startTime = System.nanoTime();
            IndexWriter indexWriter = new IndexWriter(filePaths, outputDir, connector, threads, true);
            indexWriter.performIO();

            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Elapsed time: " + elapsedTime / (1e9) + " seconds\n");

            Set<Type> entityTypes = new HashSet<>();
            Iterator<Id> idIter = indexWriter.getEntityLinker().getDictionary().elements().asIterator();

            while (idIter.hasNext())
            {
                Entity entity = indexWriter.getEntityTable().find(idIter.next());
                entityTypes.addAll(entity.getTypes());
            }

            System.out.printf("Found an approximate total of %d  unique entity mentions across %d cells %n", indexWriter.getApproximateEntityMentions(), indexWriter.cellsWithLinks());
            System.out.println("There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            System.out.println("Indexing took " + indexWriter.elapsedTime() + " ns");

            return indexWriter.loadedTables();
        }

        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }
}