package com.thetis.commands;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.thetis.loader.IndexWriter;
import com.thetis.loader.Linker;
import com.thetis.loader.LuceneLinker;
import com.thetis.loader.WikiLinker;
import com.thetis.system.Configuration;
import com.thetis.system.Logger;
import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;

import picocli.CommandLine;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import com.thetis.connector.Neo4jEndpoint;

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

    public enum Linking
    {
        WIKILINK("wikilink"), LUCENE("lucene");

        private final String type;

        Linking(String type)
        {
            this.type = type;
        }

        @Override
        public String toString()
        {
            return this.type;
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

    private String[] disallowedEntityTypes;
    @CommandLine.Option(names = {"-det", "--disallowed-types"}, paramLabel = "DISALLOWED-TYPES", description = "Disallowed entity types - use comma (',') as separator", defaultValue = "http://www.w3.org/2002/07/owl#Thing,http://www.wikidata.org/entity/Q5")
    public void setDisallowedEntityTypes(String argument)
    {
        this.disallowedEntityTypes = argument.split(",");
    }

    @CommandLine.Option(names = {"-pv", "--permutation-vectors"}, paramLabel = "PERMUTATION-VECTORS", description = "Number of permutation vectors to build entity signatures in LSH index", defaultValue = "15")
    public void setPermutationVectors(int value)
    {
        if (value <= 0)
        {
            throw new CommandLine.ParameterException(spec.commandLine(), "Number of permutation vectors must be positive");
        }

        Configuration.setPermutationVectors(value);
    }

    @CommandLine.Option(names = {"-bs", "--band-size"}, paramLabel = "BAND-SIZE", description = "Size of bands in LSH index of entity types", defaultValue = "4")
    public void setBandSize(int val)
    {
        if (val <= 1)
        {
            throw new CommandLine.ParameterException(spec.commandLine(), "Band size must be greater than 0");
        }

        Configuration.setBandSize(val);
    }

    @CommandLine.Option(names = {"-link", "--entity-linker"}, description = "Type of entity linking", required = true, defaultValue = "wikilinkg")
    private Linking linking;

    private File kgDir = null;
    @CommandLine.Option(names = {"-kg", "--kg-dir"}, paramLabel = "KG_DIR", description = "Directory of KG TTL files", required = false)
    public void setKgDir(File dir)
    {
        if (!dir.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("InvaRoyaltylid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", dir));
        }

        else if (!dir.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", dir));
        }

        this.kgDir = dir;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final String WIKI_PREFIX = "http://www.wikipedia.org/";
    private static final String URI_PREFIX = "http://dbpedia.org/";

    @Override
    public Integer call() {
        if (!embeddingsAreLoaded())
        {
            Logger.logNewLine(Logger.Level.ERROR, "Load embeddings before using this command");
            return 1;
        }

        long parsedTables;
        Logger.logNewLine(Logger.Level.INFO, "Input Directory: " + this.tableDir.getAbsolutePath());
        Logger.logNewLine(Logger.Level.INFO, "Output Directory: " + this.outputDir.getAbsolutePath());

        try {
            DBDriverBatch<List<Double>, String> embeddingStore = Factory.fromConfig(false);
            Neo4jEndpoint connector = new Neo4jEndpoint(this.configFile);
            connector.testConnection();

            Logger.logNewLine(Logger.Level.INFO, "Entity linker is constructing indexes");
            Linker linker = this.linking == Linking.LUCENE ? new LuceneLinker(connector, this.kgDir, true) : new WikiLinker(connector);
            Logger.logNewLine(Logger.Level.INFO, "Done");

            switch (this.tableType) {
                case TT ->
                        Logger.logNewLine(Logger.Level.ERROR, "Indexing of '" + TableType.TT.getName() + "' is not supported yet!");
                case WIKI -> {
                    Logger.logNewLine(Logger.Level.INFO, "Starting indexing of '" + TableType.WIKI.getName() + "'");
                    parsedTables = indexWikiTables(this.tableDir.toPath(), this.outputDir, connector, linker, embeddingStore, this.threads);
                    Logger.logNewLine(Logger.Level.INFO, "Indexed " + parsedTables + " tables\n");
                }
            }

            embeddingStore.close();
        } catch(AuthenticationException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Could not Login to Neo4j Server (user or password do not match)");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
            return 1;
        }catch (ServiceUnavailableException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Could not connect to Neo4j Server");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
            return 1;
        } catch (FileNotFoundException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Configuration file for Neo4j connector not found");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
            return 1;
        } catch (IOException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Error in reading configuration for Neo4j connector");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
            return 1;
        }

        Logger.logNewLine(Logger.Level.INFO, "DONE");
        return 0;
    }

    private boolean embeddingsAreLoaded()
    {
        try
        {
            DBDriverBatch<List<Double>, String> db = Factory.fromConfig(false);
            List<Double> embeddings;

            return (embeddings = db.select("http://dbpedia.org/ontology/team")) != null &&
                    !embeddings.isEmpty();
        }

        catch (RuntimeException e)
        {
            return false;
        }
    }

    /**
     * Returns number of tables successfully loaded
     * @param tableDir Path to directory of tables to be loaded
     * @param connector Neo4J connector instance
     * @return Number of successfully loaded tables
     */
    private long indexWikiTables(Path tableDir, File outputDir, Neo4jEndpoint connector, Linker linker,
                                DBDriverBatch<List<Double>, String> embeddingStore, int threads){

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
            Logger.logNewLine(Logger.Level.INFO, "There are " + filePaths.size() + " files to be processed.");

            long startTime = System.nanoTime();
            IndexWriter indexWriter = new IndexWriter(filePaths, outputDir, linker, connector, threads, embeddingStore,
                    WIKI_PREFIX, URI_PREFIX, this.disallowedEntityTypes);
            indexWriter.performIO();

            long elapsedTime = System.nanoTime() - startTime;
            Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + elapsedTime / (1e9) + " seconds\n");

            Set<Type> entityTypes = new HashSet<>();
            Iterator<Id> idIter = indexWriter.getEntityLinker().kgUriIds();

            while (idIter.hasNext())
            {
                Entity entity = indexWriter.getEntityTable().find(idIter.next());

                if (entity != null)
                    entityTypes.addAll(entity.getTypes());
            }

            Logger.logNewLine(Logger.Level.INFO, "Found an approximate total of " + indexWriter.getApproximateEntityMentions() + " unique entity mentions across " + indexWriter.cellsWithLinks() + " cells \n");
            Logger.logNewLine(Logger.Level.INFO, "There are in total " + entityTypes.size() + " unique entity types across all discovered entities.");
            Logger.logNewLine(Logger.Level.INFO, "Indexing took " + indexWriter.elapsedTime() + " ns");

            return indexWriter.loadedTables();
        }

        catch (IOException e)
        {
            e.printStackTrace();
            return -1;
        }
    }
}