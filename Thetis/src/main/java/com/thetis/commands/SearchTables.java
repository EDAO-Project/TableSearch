package com.thetis.commands;

import java.io.File;
import java.io.*;
import java.nio.file.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.util.*;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.IndexReader;
import com.thetis.loader.Stats;
import com.thetis.search.*;
import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.store.hnsw.HNSW;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.system.Logger;
import com.thetis.tables.JsonTable;
import com.thetis.commands.parser.TableParser;
import com.thetis.structures.Id;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import com.thetis.utilities.Utils;

import picocli.CommandLine;
import me.tongfei.progressbar.*;

@picocli.CommandLine.Command(name = "search", description = "searched the index for tables matching the input tuples")
public class SearchTables extends Command {

    //********************* Command Line Arguments *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    public enum SimilarityProperty
    {
        TYPES("types"), PREDICATES("predicates"), EMBEDDINGS("embeddings");

        private final String prop;

        SimilarityProperty(String prop) {
            this.prop = prop;
        }

        @Override
        public String toString() {
            return this.prop.toLowerCase();
        }
    }

    public enum EmbeddingSimFunction
    {
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

    public enum PrefilterTechnique {HNSW, BM25}

    @CommandLine.Option(names = {"-scpqe", "--singleColumnPerQueryEntity"}, description = "If specified, each query tuple will be evaluated against only one entity")
    private boolean singleColumnPerQueryEntity;

    @CommandLine.Option(names = {"-prop", "--kgProperty"}, description = "KG property to be used in entity similarity scoring", required = true)
    private SimilarityProperty simProperty;

    @CommandLine.Option(names = {"--hungarianAlgorithmSameAlignmentAcrossTuples"}, description = "If specified, the Hungarian algorithm uses the same alignment of columns to query entities across all query tuples")
    private boolean hungarianAlgorithmSameAlignmentAcrossTuples;

    @CommandLine.Option(names = {"-as", "--adjustedSimilarity"}, description = "If specified, the similarity score between two entities can only be 1.0 if the two entities compared are identical. " +
            "If two different entities share the same types then assign an adjusted score of < 1.0> ")
    private boolean adjustedSimilarity;

    @CommandLine.Option(names = {"-wjs", "--weightedJaccardSimilarity"}, description = "If specified, the a weighted Jaccard similarity between two entities is performed. " +
            "The weights for each entity type correspond to their respective IDF scores")
    private boolean weightedJaccardSimilarity;

    @CommandLine.Option(names = {"--useMaxSimilarityPerColumn"}, description = "If specified, instead of taking the average similarity across columns as a score the maximum value is used")
    private boolean useMaxSimilarityPerColumn;

    @CommandLine.Option(names = {"-esf", "--embeddingSimilarityFunction" }, description = "The similarity function used to compare two embedding vectors. Must be one of {norm_cos, abs_cos, ang_cos}", required = true, defaultValue="ang_cos")
    private EmbeddingSimFunction embeddingSimFunction = null;

    @CommandLine.Option(names = {"-topK", "--topK"}, description = "The top-k values to be returned when running the table search", defaultValue="100")
    private Integer topK;

    private File indexDir = null;

    @CommandLine.Option(names = {"-i", "--index-dir"}, paramLabel = "INDEX_DIR", description = "Directory of loaded indexes", defaultValue = "../data/index/wikitables/")
    public void setHashMapDirectory(File value)
    {
        if(!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--index-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--index-dir': " + "the path does not point to a directory.", value));
        }

        this.indexDir = value;
    }

    private File queriesLocation;
    private List<Path> queryFiles;

    @CommandLine.Option(names = {"-q", "--queries"}, paramLabel = "QUERY", description = "Path to directory of query json files", required = true)
    public void setQueryFile(File value)
    {
        if(!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '--queries': " + "the directory does not exists.", value));
        }

        this.queriesLocation = value;

        if (value.isFile())
        {
            this.queryFiles = List.of(value.toPath());
        }

        else
        {
            try
            {
                Stream<Path> queryStream = Files.find(value.toPath(), Integer.MAX_VALUE,
                        (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
                this.queryFiles = queryStream.collect(Collectors.toList());
            }

            catch (IOException e)
            {
                Logger.logNewLine(Logger.Level.ERROR, "Exception when finding query files: " + e.getMessage());
                System.exit(1);
            }
        }
    }

    private File tableDir = null;

    @CommandLine.Option(names = {"-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing tables", required = true)
    public void setTableDirectory(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " + "the path does not point to a directory.", value));
        }

        this.tableDir = value;
    }

    private File outputDir = null;

    @CommandLine.Option(names = {"-od", "--output-dir"}, paramLabel = "OUT_DIR", description = "Directory where to save the search results", required = true)
    public void setOutputDirectory(File value)
    {
        outputDir = value;
    }

    private File configFile = null;

    @CommandLine.Option(names = {"-cf", "--config"}, paramLabel = "CONF", description = "configuration file", required = true, defaultValue = "./config.properties" )
    public void setConfigFile(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the file does not exists.", value));
        }

        if (value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the path should point to a file not to a directory.", value));
        }

        this.configFile = value;
    }

    @CommandLine.Option(names = {"-nuri", "--neo4j-uri"}, description = "URI for Neo4J", required = false)
    private String neo4jUri = null;

    @CommandLine.Option(names = {"-nuser", "--neo4j-user"}, description = "User for Neo4J", required = false)
    private String neo4jUser = null;

    @CommandLine.Option(names = {"-npassword", "--neo4j-password"}, description = "Password for Neo4J", required = false)
    private String neo4jPassword = null;

    @CommandLine.Option(names = {"-t", "--threads"}, description = "Number of threads", required = true, defaultValue = "1")
    private int threads;

    @CommandLine.Option(names = {"-pf", "--pre-filter"}, description = "Pre-filtering technique to reduce search space (HNSW, BM25)")
    private PrefilterTechnique prefilterTechnique = null;

    @CommandLine.Option(names = {"-hk", "--hnsw-K"}, description = "Neighborhood size of HNSW search", defaultValue = "1000")
    private int hnswK;

    @Override
    public Integer call()
    {
        Logger.logNewLine(Logger.Level.INFO, "Index Directory: " + this.indexDir);
        Logger.logNewLine(Logger.Level.INFO, "Query Directory: " + this.queriesLocation.toString());
        Logger.logNewLine(Logger.Level.INFO, "Table Directory: " + this.tableDir);
        Logger.logNewLine(Logger.Level.INFO, "Output Directory: " + this.outputDir);
        Logger.logNewLine(Logger.Level.INFO, "Single Column per Query Entity: " + this.singleColumnPerQueryEntity);

        if (!this.outputDir.exists())
        {
            this.outputDir.mkdirs();
        }

        try
        {
            long startTime = System.nanoTime();
            DBDriverBatch<List<Double>, String> embeddingStore = Factory.fromConfig(false);
            Neo4jEndpoint connector = this.neo4jUri != null ? new Neo4jEndpoint(this.neo4jUri, this.neo4jUser, this.neo4jPassword) : new Neo4jEndpoint(this.configFile);
            IndexReader indexReader = new IndexReader(this.indexDir, embeddingStore, true, true);
            connector.testConnection();
            indexReader.performIO();

            long elapsedTime = System.nanoTime() - startTime;
            Logger.logNewLine(Logger.Level.INFO, "Indexes loaded from disk in " + elapsedTime / 1e9 + " seconds\n");

            EntityLinking linker = indexReader.getLinker();
            EntityTable entityTable = indexReader.getEntityTable();
            EntityTableLink entityTableLink = indexReader.getEntityTableLink();
            EmbeddingsIndex<Id> embeddingsIdx = indexReader.getEmbeddingsIndex();
            Prefilter prefilter = null;
            HNSW hnsw = indexReader.getHNSW();
            BM25 bm25 = new BM25(linker, entityTable, entityTableLink, embeddingsIdx);
            hnsw.setLinker(linker);
            hnsw.setEntityTableLink(entityTableLink);
            hnsw.setEmbeddingGenerator(entity -> embeddingStore.select(entity));
            hnsw.setK(this.hnswK);

            if (this.prefilterTechnique != null)
            {
                prefilter = switch (this.prefilterTechnique)
                {
                    case HNSW -> new Prefilter(linker, entityTable, entityTableLink, embeddingsIdx, hnsw);
                    case BM25 -> new Prefilter(linker, entityTable, entityTableLink, embeddingsIdx, bm25);
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

                if (ensureQueryEntitiesMapping(queryTable, linker, entityTableLink) ||
                        linkQueryEntities(queryTable, embeddingStore, connector, linker, entityTable, embeddingsIdx))
                    Logger.logNewLine(Logger.Level.INFO, "All query entities are mappable!\n\n");

                else
                {
                    Logger.logNewLine(Logger.Level.ERROR, "NOT all query entities are mappable! Skipping query...");
                    continue;
                }

                analogousSearch(queryTable, queryName, linker, entityTable, entityTableLink, embeddingsIdx, prefilter, this.tableDir.toPath());
            }

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

    private boolean linkQueryEntities(Table<String> query, DBDriverBatch<List<Double>, String> embeddingsDB, Neo4jEndpoint neo4j,
                                      EntityLinking linker, EntityTable entityTable, EmbeddingsIndex<Id> embeddingsIdx)
    {
        int rowCount = query.rowCount();

        for (int row = 0; row < rowCount; row++)
        {
            int rowSize = query.getRow(row).size();

            for (int column = 0; column < rowSize; column++)
            {
                String entity = query.getRow(row).get(column), link = linker.getInputPrefix() + "q" + row + column;
                List<String> entityTypes = neo4j.searchTypes(entity);
                List<String> entityPredicates = neo4j.searchPredicates(entity);
                linker.addMapping(link, entity);

                Id entityId = linker.kgUriLookup(entity);
                List<Double> embeddings = embeddingsDB.select(entity.replace("'", "''"));
                entityTable.insert(entityId,
                        new Entity(entity, entityTypes.stream().map(Type::new).collect(Collectors.toList()), entityPredicates));

                if (embeddings != null)
                {
                    embeddingsIdx.insert(entityId, embeddings);
                }
            }
        }

        return true;
    }

    /**
     * Given a list of entities, return a ranked list of table candidates
     */
    public void analogousSearch(Table<String> query, String queryName, EntityLinking linker, EntityTable table,
                                EntityTableLink tableLink, EmbeddingsIndex<Id> embeddingIdx, Prefilter prefilter,
                                Path tableDir) throws IOException
    {
        AnalogousSearch search;
        Stream<Path> fileStream = Files.find(tableDir, Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
        Set<String> filePaths = fileStream.map(Path::toString).collect(Collectors.toSet());
        AnalogousSearch.EntitySimilarity entitySimilarity = this.simProperty == SimilarityProperty.TYPES ?
                AnalogousSearch.EntitySimilarity.JACCARD_TYPES : AnalogousSearch.EntitySimilarity.JACCARD_PREDICATES;

        if (this.simProperty == SimilarityProperty.EMBEDDINGS)
        {
            entitySimilarity = this.embeddingSimFunction == EmbeddingSimFunction.ABS_COS
                    ? AnalogousSearch.EntitySimilarity.EMBEDDINGS_ABS : this.embeddingSimFunction == EmbeddingSimFunction.NORM_COS
                    ? AnalogousSearch.EntitySimilarity.EMBEDDINGS_NORM : AnalogousSearch.EntitySimilarity.EMBEDDINGS_ANG;
        }

        if (prefilter == null)
        {
            search = new AnalogousSearch(filePaths, linker, table, tableLink, embeddingIdx, this.topK, this.threads, entitySimilarity,
                    this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedSimilarity, this.useMaxSimilarityPerColumn,
                    this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN);
        }

        else
        {
            search = new AnalogousSearch(filePaths, linker, table, tableLink, embeddingIdx, this.topK, this.threads, entitySimilarity,
                    this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedSimilarity, this.useMaxSimilarityPerColumn,
                    this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN, prefilter);
        }

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
                search.getNonEmbeddingComparisons(), search.getEmbeddingCoverageSuccesses(), search.getEmbeddingCoverageFails(),
                search.getReduction(), this.embeddingSimFunction, this.simProperty, this.prefilterTechnique, this.singleColumnPerQueryEntity,
                this.useMaxSimilarityPerColumn, this.adjustedSimilarity, this.threads);
    }

    /**
     * Saves the data of the filenameToScore Hashmap into the "filenameToScore.json" file at the specified output directory
     */
    public static void saveFilenameScores(File outputDir, String tableDir, String queryName, List<Pair<String, Double>> scores,
                                                Map<String, Stats> tableStats, Set<String> queryEntitiesMissingCoverage,
                                                long runtime, int embeddingComparisons, int nonEmbeddingComparisons,
                                                int embeddingCoverageSuccesses, int embeddingCoverageFails, double reduction,
                                                EmbeddingSimFunction embeddingSimFunction, SimilarityProperty prop, PrefilterTechnique prefilterTechnique,
                                                boolean singleColumnPerQueryEntity, boolean useMaxSimilarityPerColumn, boolean adjustedSimilarity,
                                                int threads)
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
        for (Pair<String, Double> score : ProgressBar.wrap(scores, "Processing files..."))
        {
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
            if (tableStats.containsKey(score.getFirst()))
            {
                tmp.addProperty("numEntityMappedRows", String.valueOf(tableStats.get(score.getFirst()).entityMappedRows()));
                tmp.addProperty("fractionOfEntityMappedRows", String.valueOf(tableStats.get(score.getFirst()).fractionOfEntityMappedRows()));
                tmp.addProperty("tupleScores", String.valueOf(tableStats.get(score.getFirst()).queryRowScores()));
                tmp.addProperty("tupleVectors", String.valueOf(tableStats.get(score.getFirst()).queryRowVectors()));
            }

            if (singleColumnPerQueryEntity && tableStats.containsKey(score.getFirst()))
                tmp.addProperty("tuple_query_alignment", String.valueOf(tableStats.get(score.getFirst()).tupleQueryAlignment()));

            innerObjs.add(tmp);
        }

        jsonObj.add("scores", innerObjs);

        // Runtime to process all tables (does not consider time to compute scores) and algorithm to compute the results
        String algorithm = (prefilterTechnique != null ? prefilterTechnique.name() + " " : "") + "brute-force with " +
                (useMaxSimilarityPerColumn ? "max similarity per column aggregation" : "average similarity per column aggregation") +
                " (" + (prop == SimilarityProperty.EMBEDDINGS ? "embeddings - " + embeddingSimFunction.name() : "types - " +
                (adjustedSimilarity ? "with" : "without") + " adjusted entity similarity") + ")";
        jsonObj.addProperty("runtime", runtime);
        jsonObj.addProperty("reduction", reduction);
        jsonObj.addProperty("threads", threads);
        jsonObj.addProperty("algorithm", algorithm);

        if (prop == SimilarityProperty.EMBEDDINGS) {
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
