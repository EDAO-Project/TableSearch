package dk.aau.cs.daisy.edao.commands;

import java.io.File;
import java.io.*;
import java.nio.file.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.*;
import dk.aau.cs.daisy.edao.loader.IndexReader;
import dk.aau.cs.daisy.edao.loader.Stats;
import dk.aau.cs.daisy.edao.search.*;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.store.lsh.TypesLSHIndex;
import dk.aau.cs.daisy.edao.store.lsh.VectorLSHIndex;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.system.Logger;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.Ppr;
import dk.aau.cs.daisy.edao.utilities.Utils;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import picocli.CommandLine;
import me.tongfei.progressbar.*;

@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {

    //********************* Command Line Arguments *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    private enum SearchMode {
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

    private enum EmbeddingSimFunction {
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

    private enum PrefilterTechnique {LSH_TYPES, LSH_EMBEDDINGS, PPR}

    @CommandLine.Option(names = { "-sm", "--search-mode" }, description = "Must be one of {exact, analogous}", required = true)
    private SearchMode searchMode = null;

    @CommandLine.Option(names = { "-scpqe", "--singleColumnPerQueryEntity"}, description = "If specified, each query tuple will be evaluated against only one entity")
    private boolean singleColumnPerQueryEntity;

    @CommandLine.Option(names = { "-upe", "--usePretrainedEmbeddings"}, description = "If specified, pre-trained embeddings are used to capture the similarity between two entities whenever possible")
    private boolean usePretrainedEmbeddings;

    @CommandLine.Option(names = { "--hungarianAlgorithmSameAlignmentAcrossTuples"}, description = "If specified, the Hungarian algorithm uses the same alignment of columns to query entities across all query tuples")
    private boolean hungarianAlgorithmSameAlignmentAcrossTuples;

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

    private File queriesLocation;
    private List<Path> queryFiles;
    @CommandLine.Option(names = { "-q", "--queries" }, paramLabel = "QUERY", description = "Path to directory of query json files", required = true)
    public void setQueryFile(File value) {
        if(!value.exists()){
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--queries': " + "the directory does not exists.", value));
        }

        this.queriesLocation = value;

        if (value.isFile()) {
            this.queryFiles = List.of(value.toPath());
        }

        else {
            try {
                Stream<Path> queryStream = Files.find(value.toPath(), Integer.MAX_VALUE,
                        (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
                this.queryFiles = queryStream.collect(Collectors.toList());
            }
            catch (IOException e) {
                Logger.logNewLine(Logger.Level.ERROR, "Exception when finding query files: " + e.getMessage());
                System.exit(1);
            }
        }
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

    @CommandLine.Option(names = {"-pf", "--pre-filter"}, description = "Pre-filtering technique to reduce search space (LSH_TYPES, LSH_EMBEDDINGS, PPR)")
    private PrefilterTechnique prefilterTechnique = null;

    // Initialize a connection with the embeddings Database
    private DBDriverBatch<List<Double>, String> store;

    @Override
    public Integer call()
    {
        Logger.logNewLine(Logger.Level.INFO, "Index Directory: " + this.indexDir);
        Logger.logNewLine(Logger.Level.INFO, "Query Directory: " + this.queriesLocation.toString());
        Logger.logNewLine(Logger.Level.INFO, "Table Directory: " + this.tableDir);
        Logger.logNewLine(Logger.Level.INFO, "Output Directory: " + this.outputDir);
        Logger.logNewLine(Logger.Level.INFO, "Single Column per Query Entity: " + this.singleColumnPerQueryEntity);

        // Create output directory if it doesn't exist
        if (!this.outputDir.exists())
            this.outputDir.mkdirs();

        try
        {
            this.store = Factory.fromConfig(false);

            // Perform De-Serialization of the indexes
            long startTime = System.nanoTime();
            IndexReader indexReader = new IndexReader(this.indexDir, true, true);
            indexReader.performIO();

            long elapsedTime = System.nanoTime() - startTime;
            Logger.logNewLine(Logger.Level.INFO, "Indexes loaded from disk in " + elapsedTime / 1e9 + " seconds\n");

            EntityLinking linker = indexReader.getLinker();
            EntityTable entityTable = indexReader.getEntityTable();
            EntityTableLink entityTableLink = indexReader.getEntityTableLink();
            TypesLSHIndex typesLSH = indexReader.getTypesLSHIndex();
            VectorLSHIndex embeddingsLSH = indexReader.getEmbeddingsLSHIndex();
            typesLSH.useEntityLinker(linker);
            embeddingsLSH.useEntityLinker(linker);
            Prefilter prefilter = null;

            if (this.prefilterTechnique != null)
            {
                prefilter = switch (this.prefilterTechnique) {    // TODO: PPR pre-filtering must be implemented
                    case LSH_TYPES -> new Prefilter(linker, entityTable, entityTableLink, typesLSH);
                    case LSH_EMBEDDINGS -> new Prefilter(linker, entityTable, entityTableLink, embeddingsLSH);
                    default -> null;
                };
            }

            for (Path queryPath : this.queryFiles)
            {
                String[] split = queryPath.toFile().toString().split("/");
                String queryName = split[split.length - 1].split("\\.")[0];
                Table<String> queryTable = TableParser.toTable(queryPath.toFile());

                if (queryTable == null)
                {
                    Logger.logNewLine(Logger.Level.ERROR, "Query file '" + queryPath.toFile() + "' could not be parsed");
                    return -1;
                }

                Logger.logNewLine(Logger.Level.INFO, "Query Entities: " + queryTable + "\n");

                if (ensureQueryEntitiesMapping(queryTable, linker, entityTableLink))
                    Logger.logNewLine(Logger.Level.INFO, "All query entities are mappable!\n\n");

                else
                {
                    Logger.logNewLine(Logger.Level.ERROR, "NOT all query entities are mappable! Skipping query...");
                    continue;
                }

                Logger.logNewLine(Logger.Level.INFO, "Search mode: " + this.searchMode.getMode());

                switch (this.searchMode)
                {
                    case EXACT:
                        exactSearch(queryTable, linker, entityTable, entityTableLink);
                        break;

                    case ANALOGOUS:
                        analogousSearch(queryTable, queryName, linker, entityTable, entityTableLink, prefilter, this.tableDir.toPath());
                        break;

                    case PPR:
                        ppr(queryTable, queryName, linker, entityTable, entityTableLink);
                        break;
                }
            }

            this.store.close();
            return 0;
        }

        catch (IOException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Failed to load indexes from disk");
            return -1;
        }

        catch (RuntimeException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, e.getMessage());
            return -1;
        }
    }

    public boolean ensureQueryEntitiesMapping(Table<String> query, EntityLinking linker, EntityTableLink tableLink)
    {
        int rows = query.rowCount();

        for (int i = 0; i < rows; i++)
        {
            Table.Row<String> row = query.getRow(i);
            int rowSize = row.size();

            for (int j = 0; j < rowSize; j++)
            {
                Id entityId = linker.kgUriLookup(row.get(j));

                if (!tableLink.contains(entityId))
                {
                    return false;
                }
            }
        }

        return true;
    }

    public void exactSearch(Table<String> query, EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink)
    {
        Iterator<Id> entityIter = linker.kgUriIds();

        while (entityIter.hasNext())
        {
            Id entityId = entityIter.next();
            String entity = linker.kgUriLookup(entityId);
            List<String> tableFiles = entityTableLink.find(entityId);

            if (tableFiles == null || tableFiles.isEmpty())
            {
                Logger.logNewLine(Logger.Level.RESULT, "'" + entity + "' does not map to any known entity in the constructed index");
            }

            else
            {
                Logger.logNewLine(Logger.Level.RESULT, "There are " + tableFiles.size() + " files that contain the entity '" + entity + "'");
            }
        }

        TableSearch search = new ExactSearch(linker, entityTable, entityTableLink);
        Iterator<Pair<String, Double>> resIter = search.search(query).getResults();

        while (resIter.hasNext())
        {
            Pair<String, Double> result = resIter.next();
            Logger.logNewLine(Logger.Level.RESULT, "For filename '" + result.getFirst() + "', there are " + result.getSecond() + " matching tuples");
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
        Logger.logNewLine(Logger.Level.RESULT, "\n2-entities analysis:");

        for (int i = 0; i < tableEntities; i++)
        {
            for (int j = 0; j < tableEntities; j++)
            {
                resIter = search.search(new DynamicTable<>(List.of(flattenedQueryTable))).getResults();

                while (resIter.hasNext())
                {
                    Pair<String, Double> result = resIter.next();
                    Logger.logNewLine(Logger.Level.RESULT, "For filename '" + result.getFirst() + "', there are " + result.getSecond() + " matching tuples");
                }
            }
        }
    }

    /**
     * Given a list of entities, return a ranked list of table candidates
     */
    public void analogousSearch(Table<String> query, String queryName, EntityLinking linker, EntityTable table,
                                EntityTableLink tableLink, Prefilter prefilter, Path tableDir) throws IOException
    {
        AnalogousSearch search;
        Stream<Path> fileStream = Files.find(tableDir, Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
        Set<Path> filePaths = fileStream.collect(Collectors.toSet());
        filePaths = filePaths.stream().map(Path::toAbsolutePath).collect(Collectors.toSet());
        AnalogousSearch.CosineSimilarityFunction cosineFunction = this.embeddingSimFunction == EmbeddingSimFunction.ABS_COS
                ? AnalogousSearch.CosineSimilarityFunction.ABS_COS : this.embeddingSimFunction == EmbeddingSimFunction.NORM_COS
                ? AnalogousSearch.CosineSimilarityFunction.NORM_COS : AnalogousSearch.CosineSimilarityFunction.ANG_COS;

        if (prefilter == null)
        {
            search = new AnalogousSearch(linker, table, tableLink, this.topK, this.threads, this.usePretrainedEmbeddings,
                    cosineFunction, this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedJaccardSimilarity,
                    this.useMaxSimilarityPerColumn, this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN, this.store);
        }

        else
        {
            search = new AnalogousSearch(linker, table, tableLink, this.topK, this.threads, this.usePretrainedEmbeddings,
                    cosineFunction, this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedJaccardSimilarity,
                    this.useMaxSimilarityPerColumn, this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN,
                    this.store, prefilter);
        }

        search.setCorpus(filePaths.stream().map(Path::toString).collect(Collectors.toSet()));

        Result result = search.search(query);
        Iterator<Pair<String, Double>> resultIter = result.getResults();
        List<Pair<String, Double>> scores = new ArrayList<>();
        Logger.logNewLine(Logger.Level.RESULT, "\nTop-" + this.topK + " tables are:");

        while (resultIter.hasNext())
        {
            Pair<String, Double> next = resultIter.next();
            scores.add(next);
            Logger.logNewLine(Logger.Level.RESULT, "Filename = " + next.getFirst() + ", score = " + next.getSecond());
        }

        saveFilenameScores(this.outputDir, tableLink.getDirectory(), queryName, scores, search.getTableStats(),
                search.getQueryEntitiesMissingCoverage(), search.elapsedNanoSeconds(), search.getEmbeddingComparisons(),
                search.getNonEmbeddingComparisons(), search.getEmbeddingCoverageSuccesses(), search.getEmbeddingCoverageFails());
    }

    public int ppr(Table<String> query, String queryName, EntityLinking linker, EntityTable table, EntityTableLink tableLink) {
        // Initialize the connector
        try {
            Neo4jEndpoint neo4j = new Neo4jEndpoint(this.configFile);
            neo4j.testConnection();


            if (this.pprSingleRequestForAllQueryTuples)
            {
                query = Ppr.combineQueryTuplesInSingleTuple(query);
            }

            PPRSearch search = new PPRSearch(linker, table, tableLink, neo4j, this.weightedPPR, this.minThreshold,
                    this.numParticles, this.topK);
            Result result = search.search(query);
            Iterator<Pair<String, Double>> resultIter = result.getResults();
            List<Pair<String, Double>> scores = new ArrayList<>();

            while (resultIter.hasNext())
            {
                Pair<String, Double> next = resultIter.next();
                scores.add(next);
                Logger.logNewLine(Logger.Level.RESULT, "Filename = " + next.getFirst() + ", score = " + next.getSecond());
            }

            saveFilenameScores(this.outputDir, tableLink.getDirectory(), queryName, scores, new HashMap<>(), Set.of(), search.elapsedNanoSeconds(), -1, -1, -1, -1);
        } catch(AuthenticationException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Could not Login to Neo4j Server (user or password do not match)");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
        }catch (ServiceUnavailableException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Could not connect to Neo4j Server");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
        } catch (FileNotFoundException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Configuration file for Neo4j connector not found");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
        } catch ( IOException ex){
            Logger.logNewLine(Logger.Level.ERROR, "Error in reading configuration for Neo4j connector");
            Logger.logNewLine(Logger.Level.ERROR, ex.getMessage());
        }

        return 1;
    }

    /**
     * Saves the data of the filenameToScore Hashmap into the "filenameToScore.json" file at the specified output directory
     */
    // TODO: Wrap this in RAII for better control
    public synchronized void saveFilenameScores(File outputDir, String tableDir, String queryName, List<Pair<String, Double>> scores,
                                                Map<String, Stats> tableStats, Set<String> queryEntitiesMissingCoverage,
                                                long runtime, int embeddingComparisons, int nonEmbeddingComparisons,
                                                int embeddingCoverageSuccesses, int embeddingCoverageFails)
    {
        File saveDir = new File(outputDir, "/search_output/" + queryName);

        if (!saveDir.exists())
        {
            saveDir.mkdirs();
        }

        Logger.logNewLine(Logger.Level.INFO, "\nConstructing the filenameToScore.json file...");

        // Specify the format of the filenameToScore.json file 
        JsonObject jsonObj = new JsonObject();
        JsonArray innerObjs = new JsonArray();
        
        // Iterate over filenameToScore hashmap
        for (Pair<String, Double> score : ProgressBar.wrap(scores, "Processing files...")) {
            JsonObject tmp = new JsonObject();
            tmp.addProperty("tableID", score.getFirst());
            tmp.addProperty("score", score.getSecond());
            
            // Get Page Title and URL of the current file
            JsonTable table = Utils.getTableFromPath(Paths.get(tableDir + score.getFirst()));
            String pgTitle = table.pgTitle;
            String tableURL = "https://en.wikipedia.org/wiki/"+pgTitle.replace(' ', '_');
            tmp.addProperty("pgTitle", pgTitle);
            tmp.addProperty("tableURL", tableURL);

            // Add Statistics for current filename
            if (!this.searchMode.getMode().equals("ppr")) {
                tmp.addProperty("numEntityMappedRows", String.valueOf(tableStats.get(score.getFirst()).entityMappedRows()));
                tmp.addProperty("fractionOfEntityMappedRows", String.valueOf(tableStats.get(score.getFirst()).fractionOfEntityMappedRows()));
                tmp.addProperty("tupleScores", String.valueOf(tableStats.get(score.getFirst()).queryRowScores()));
                tmp.addProperty("tupleVectors", String.valueOf(tableStats.get(score.getFirst()).queryRowVectors()));
            }

            if (this.singleColumnPerQueryEntity)
                tmp.addProperty("tuple_query_alignment", String.valueOf(tableStats.get(score.getFirst()).tupleQueryAlignment()));

            innerObjs.add(tmp);
        }
        jsonObj.add("scores", innerObjs);

        // Runtime to process all tables (does not consider time to compute scores)
        jsonObj.addProperty("runtime", runtime);

        if (this.usePretrainedEmbeddings) {
            // Add the embedding statistics
            jsonObj.addProperty("numEmbeddingSimComparisons", embeddingComparisons);
            jsonObj.addProperty("numNonEmbeddingSimComparisons", nonEmbeddingComparisons);
            jsonObj.addProperty("fractionOfEntityMappedRows", (double) embeddingComparisons / (embeddingComparisons + nonEmbeddingComparisons));

            jsonObj.addProperty("numEmbeddingCoverageSuccesses", embeddingCoverageSuccesses);
            jsonObj.addProperty("numEmbeddingCoverageFails", embeddingCoverageFails);
            jsonObj.addProperty("embeddingCoverageSuccessRate", (double) embeddingCoverageSuccesses / (embeddingCoverageSuccesses + embeddingCoverageFails));

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

        Logger.logNewLine(Logger.Level.INFO, "Finished constructing the filenameToScore.json file.");
    }
}
