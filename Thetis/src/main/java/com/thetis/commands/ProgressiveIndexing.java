package com.thetis.commands;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.Linker;
import com.thetis.loader.LuceneLinker;
import com.thetis.loader.WikiLinker;
import com.thetis.loader.progressive.PriorityScheduler;
import com.thetis.loader.progressive.ProgressiveIndexWriter;
import com.thetis.search.*;
import com.thetis.store.hnsw.HNSW;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import com.thetis.system.Logger;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@picocli.CommandLine.Command(name = "progressive", description = "progressively creates the index for the specified set of tables")
public class ProgressiveIndexing extends Command
{
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

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

    private File tableDir = null;

    @CommandLine.Option(names = {"-td", "--table-dir"}, paramLabel = "TABLE_DIR", description = "Directory containing the input tables", required = true)
    public void setTableDirectory(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("InvaRoyaltylid value '%s' for option '--table-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }

        this.tableDir = value;
    }

    private File outputDir = null;

    @CommandLine.Option(names = {"-od", "--output-dir"}, paramLabel = "OUT_DIR", description = "Directory where the index and its metadata are saved", required = true)
    public void setOutputDirectory(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(this.spec.commandLine(),
                    String.format("Invalid value '%s' for option '--output-dir': " +
                            "the directory does not exists.", value));
        }

        if (!value.isDirectory())
        {
            throw new CommandLine.ParameterException(this.spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the path does not point to a directory.", value));
        }

        if (!value.canWrite())
        {
            throw new CommandLine.ParameterException(this.spec.commandLine(),
                    String.format("Invalid value '%s' for option '--table-dir': " +
                            "the directory is not writable.", value));
        }

        this.outputDir = value;
    }

    private String[] disallowedEntityTypes;

    @CommandLine.Option(names = {"-det", "--disallowed-types"}, paramLabel = "DISALLOWED-TYPES", description = "Disallowed entity types - use comma (',') as separator", defaultValue = "http://www.w3.org/2002/07/owl#Thing,http://www.wikidata.org/entity/Q5")
    public void setDisallowedEntityTypes(String argument)
    {
        this.disallowedEntityTypes = argument.split(",");
    }

    @CommandLine.Option(names = {"-link", "--entity-linker"}, description = "Type of entity linking", required = true, defaultValue = "wikilink")
    private IndexTables.Linking linking;

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

    @CommandLine.Option(names = {"-scpqe", "--singleColumnPerQueryEntity"}, description = "If specified, each query tuple will be evaluated against only one entity")
    private boolean singleColumnPerQueryEntity;

    @CommandLine.Option(names = {"-prop", "--kgProperty"}, description = "KG property to be used in entity similarity scoring", required = true)
    private SearchTables.SimilarityProperty simProperty;

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
    private SearchTables.EmbeddingSimFunction embeddingSimFunction = null;

    @CommandLine.Option(names = {"-topK", "--topK"}, description = "The top-k values to be returned when running the table search", defaultValue="100")
    private Integer topK;

    private File resultDir = null;

    @CommandLine.Option(names = {"-rd", "--result-dir"}, paramLabel = "RESULT_DIR", description = "Directory where to save the search results", required = true)
    public void setResultDirectory(File value)
    {
        this.resultDir = value;
    }

    @CommandLine.Option(names = {"-it", "--indexing-time"}, description = "Threshold indexing time (seconds) before the query is executed", defaultValue="0")
    private int indexingTime;

    @CommandLine.Option(names = {"-pf", "--pre-filter"}, description = "Pre-filtering technique to reduce search space (HNSW, BM25)")
    private SearchTables.PrefilterTechnique prefilterTechnique = null;

    @CommandLine.Option(names = {"-hk", "--hnsw-K"}, description = "Neighborhood size of HNSW search", defaultValue = "1000")
    private int hnswK;

    @Override
    public Integer call()
    {
        if (!IndexTables.embeddingsAreLoaded())
        {
            Logger.logNewLine(Logger.Level.ERROR, "Load embeddings before using this command");
            return 1;
        }

        Logger.logNewLine(Logger.Level.INFO, "Input Directory: " + this.tableDir.getAbsolutePath());
        Logger.logNewLine(Logger.Level.INFO, "Output Directory: " + this.outputDir.getAbsolutePath());

        try
        {
            File queryDir = new File("/queries/");
            long start = System.currentTimeMillis();
            DBDriverBatch<List<Double>, String> embeddingStore = Factory.fromConfig(false);
            Neo4jEndpoint connector = this.neo4jUri != null ? new Neo4jEndpoint(this.neo4jUri, this.neo4jUser, this.neo4jPassword) : new Neo4jEndpoint(this.configFile);
            connector.testConnection();

            Logger.logNewLine(Logger.Level.INFO, "Entity linker is constructing indexes");
            Linker linker = this.linking == IndexTables.Linking.LUCENE ? new LuceneLinker(connector, this.kgDir, true) : new WikiLinker(connector);
            Logger.logNewLine(Logger.Level.INFO, "Done");
            Logger.logNewLine(Logger.Level.INFO, "Starting progressive indexing");

            Stream<Path> fileStream = Files.find(this.tableDir.toPath(), Integer.MAX_VALUE,
                    (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
            List<Path> filePaths = fileStream.collect(Collectors.toList());
            Set<String> searchTables = filePaths.stream().map(path -> path.getFileName().toString()).collect(Collectors.toSet());
            Collections.sort(filePaths);
            Logger.logNewLine(Logger.Level.INFO, "There are " + filePaths.size() + " files to be processed.");

            AnalogousSearch.EntitySimilarity entitySimilarity = this.simProperty == SearchTables.SimilarityProperty.TYPES ?
                    AnalogousSearch.EntitySimilarity.JACCARD_TYPES : AnalogousSearch.EntitySimilarity.JACCARD_PREDICATES;

            if (this.simProperty == SearchTables.SimilarityProperty.EMBEDDINGS)
            {
                entitySimilarity = this.embeddingSimFunction == SearchTables.EmbeddingSimFunction.ABS_COS
                        ? AnalogousSearch.EntitySimilarity.EMBEDDINGS_ABS : this.embeddingSimFunction == SearchTables.EmbeddingSimFunction.NORM_COS
                        ? AnalogousSearch.EntitySimilarity.EMBEDDINGS_NORM : AnalogousSearch.EntitySimilarity.EMBEDDINGS_ANG;
            }

            Runnable cleanup = () -> {
                long elapsed = System.currentTimeMillis() - start;
                embeddingStore.close();
                Logger.log(Logger.Level.INFO, "Progressively loaded in " + (elapsed / 1000) / 60 + " minutes");
            };
            QueryRetriever queryRetriever = new QueryRetriever(queryDir);
            ProgressiveIndexWriter indexWriter = new ProgressiveIndexWriter(filePaths, this.outputDir, linker, connector,
                    1, embeddingStore, IndexTables.WIKI_PREFIX, IndexTables.URI_PREFIX, new PriorityScheduler(), cleanup);
            indexWriter.performIO();

            HNSW hnsw = indexWriter.getHNSW();
            BM25 bm25 = new BM25(indexWriter.getEntityLinker(), indexWriter.getEntityTable(), indexWriter.getEntityTableLinker(),
                    indexWriter.getEmbeddingsIndex());
            hnsw.setK(this.hnswK);

            Prefilter bm25Prefilter = new Prefilter(indexWriter.getEntityLinker(), indexWriter.getEntityTable(), indexWriter.getEntityTableLinker(),
                    indexWriter.getEmbeddingsIndex(), bm25), hnswPrefilter = new Prefilter(indexWriter.getEntityLinker(),
                    indexWriter.getEntityTable(), indexWriter.getEntityTableLinker(), indexWriter.getEmbeddingsIndex(), hnsw);

            while (true)
            {
                try
                {
                    var query = queryRetriever.next();
                    File queryFile = query.getKey();
                    Table<String> queryTable = query.getRight();
                    TimeUnit.SECONDS.sleep(this.indexingTime);
                    indexWriter.pauseIndexing();

                    AnalogousSearch search = switch (this.prefilterTechnique) {
                        case BM25 -> new AnalogousSearch(searchTables, indexWriter.getEntityLinker(), indexWriter.getEntityTable(),
                                indexWriter.getEntityTableLinker(), indexWriter.getEmbeddingsIndex(), this.topK, 1 ,entitySimilarity,
                                this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedSimilarity, this.useMaxSimilarityPerColumn,
                                this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN, bm25Prefilter);
                        case HNSW -> new AnalogousSearch(searchTables, indexWriter.getEntityLinker(), indexWriter.getEntityTable(),
                                indexWriter.getEntityTableLinker(), indexWriter.getEmbeddingsIndex(), this.topK, 1, entitySimilarity,
                                this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedSimilarity, this.useMaxSimilarityPerColumn,
                                this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN, hnswPrefilter);
                        default -> new AnalogousSearch(searchTables, indexWriter.getEntityLinker(), indexWriter.getEntityTable(),   // TODO: Check if this is selected if no pre-filter technique is selected
                                indexWriter.getEntityTableLinker(), indexWriter.getEmbeddingsIndex(), this.topK, 1, entitySimilarity,
                                this.singleColumnPerQueryEntity, this.weightedJaccardSimilarity, this.adjustedSimilarity, this.useMaxSimilarityPerColumn,
                                this.hungarianAlgorithmSameAlignmentAcrossTuples, AnalogousSearch.SimilarityMeasure.EUCLIDEAN);
                    };

                    Result results = search.search(queryTable);
                    Iterator<Pair<String, Double>> resultIter = results.getResults();
                    List<Pair<String, Double>> scores = new ArrayList<>();
                    indexWriter.continueIndexing();

                    while (resultIter.hasNext())
                    {
                        Pair<String, Double> result = resultIter.next();    // TODO: Check if result.getFirst() returns the ID of an indexable
                        double alpha = 1;
                        double newPriority = indexWriter.getMaxPriority() * (1 + result.getSecond() * alpha);
                        indexWriter.updatePriority(result.getFirst(), (i) -> i.setPriority(newPriority));
                        scores.add(result);
                    }

                    SearchTables.saveFilenameScores(this.outputDir, indexWriter.getEntityTableLinker().getDirectory(),
                            queryFile.getName().split("\\.")[0], scores, search.getTableStats(), search.getQueryEntitiesMissingCoverage(),
                            search.elapsedNanoSeconds(), search.getEmbeddingComparisons(), search.getNonEmbeddingComparisons(),
                            search.getEmbeddingCoverageSuccesses(), search.getEmbeddingCoverageFails(), search.getReduction(),
                            this.embeddingSimFunction, this.simProperty, this.prefilterTechnique, this.singleColumnPerQueryEntity,
                            this.useMaxSimilarityPerColumn, this.adjustedSimilarity, 1);
                }

                catch (InterruptedException e)
                {
                    Logger.log(Logger.Level.ERROR, "Sleeping error during search: " + e.getMessage());
                }
            }
        }

        catch (IOException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Error in reading configuration for Neo4j connector");
            Logger.logNewLine(Logger.Level.ERROR, e.getMessage());
            return 1;
        }
    }
}
