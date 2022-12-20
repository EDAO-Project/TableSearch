package dk.aau.cs.daisy.edao.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.DBDriverBatch;
import dk.aau.cs.daisy.edao.loader.Stats;
import dk.aau.cs.daisy.edao.similarity.JaccardSimilarity;
import dk.aau.cs.daisy.edao.store.EmbeddingsIndex;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.system.Logger;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.HungarianAlgorithm;
import dk.aau.cs.daisy.edao.utilities.Utils;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Entry class for searching tables using our algorithm
 */
public class AnalogousSearch extends AbstractSearch
{
    public enum SimilarityMeasure
    {
        COSINE("cosine"), EUCLIDEAN("euclidean");

        private String measure;

        SimilarityMeasure(String measure)
        {
            this.measure = measure;
        }

        @Override
        public String toString()
        {
            return this.measure;
        }

        public boolean equals(SimilarityMeasure other)
        {
            return toString().equals(other.toString());
        }
    }

    public enum CosineSimilarityFunction {NORM_COS, ABS_COS, ANG_COS}

    private int topK, threads, embeddingComparisons, nonEmbeddingComparisons,
            embeddingCoverageSuccesses, embeddingCoverageFails;
    Set<String> queryEntitiesMissingCoverage = new HashSet<>();
    private long elapsed = -1, parsedTables;
    private boolean useEmbeddings, singleColumnPerQueryEntity, weightedJaccard, adjustedJaccard,
            useMaxSimilarityPerColumn, hungarianAlgorithmSameAlignmentAcrossTuples;
    private CosineSimilarityFunction embeddingSimFunction;
    private SimilarityMeasure measure;
    private DBDriverBatch<List<Double>, String> embeddingsDB;
    private Map<String, Stats> tableStats = new TreeMap<>();
    private final Object lockStats = new Object();
    private Set<String> corpus;
    private Prefilter prefilter;
    private Map<String, List<Double>> queryEntityEmbeddings = null;
    private final EmbeddingsIndex<String> embeddingsIndex = new EmbeddingsIndex();
    private final Cache<String, List<Double>> embeddingsCache = CacheBuilder.newBuilder().maximumSize(10000).build();

    public AnalogousSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, int topK,
                           int threads, boolean useEmbeddings, CosineSimilarityFunction cosineFunction,
                           boolean singleColumnPerQueryEntity, boolean weightedJaccard, boolean adjustedJaccard,
                           boolean useMaxSimilarityPerColumn, boolean hungarianAlgorithmSameAlignmentAcrossTuples,
                           SimilarityMeasure similarityMeasure, DBDriverBatch<List<Double>, String> embeddingStore)
    {
        super(linker, entityTable, entityTableLink);
        this.topK = topK;
        this.threads = threads;
        this.useEmbeddings = useEmbeddings;
        this.embeddingSimFunction = cosineFunction;
        this.singleColumnPerQueryEntity = singleColumnPerQueryEntity;
        this.weightedJaccard = weightedJaccard;
        this.adjustedJaccard = adjustedJaccard;
        this.hungarianAlgorithmSameAlignmentAcrossTuples = hungarianAlgorithmSameAlignmentAcrossTuples;
        this.useMaxSimilarityPerColumn = useMaxSimilarityPerColumn;
        this.measure = similarityMeasure;
        this.embeddingsDB = embeddingStore;
        this.corpus = distinctTables();
        this.prefilter = null;
    }

    public AnalogousSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, int topK,
                           int threads, boolean useEmbeddings, CosineSimilarityFunction cosineFunction,
                           boolean singleColumnPerQueryEntity, boolean weightedJaccard, boolean adjustedJaccard,
                           boolean useMaxSimilarityPerColumn, boolean hungarianAlgorithmSameAlignmentAcrossTuples,
                           SimilarityMeasure similarityMeasure, DBDriverBatch<List<Double>, String> embeddingStore,
                           Prefilter prefilter)
    {
        this(linker, entityTable, entityTableLink, topK, threads, useEmbeddings, cosineFunction, singleColumnPerQueryEntity,
                weightedJaccard, adjustedJaccard, useMaxSimilarityPerColumn, hungarianAlgorithmSameAlignmentAcrossTuples,
                similarityMeasure, embeddingStore);
        this.prefilter = prefilter;
    }

    private void setQueryEntityEmbeddings(Table<String> query)
    {
        List<String> entities = new ArrayList<>();
        int queryRows = query.rowCount();

        for (int row = 0; row < queryRows; row++)
        {
            int columns = query.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                entities.add(query.getRow(row).get(column));
            }
        }

        this.queryEntityEmbeddings = new ConcurrentHashMap<>(this.embeddingsDB.batchSelect(entities));
    }

    private void insertTableEntityEmbeddings(JsonTable table, String tableId)
    {
        List<String> entities = new ArrayList<>();

        table.forEach(cell -> {
            for (String link : cell.links)
            {
                String uri = getLinker().mapTo(link);

                if (uri != null)
                {
                    List<Double> embedding = this.embeddingsCache.getIfPresent(uri);

                    if (embedding != null)
                    {
                        this.embeddingsIndex.clusterInsert(tableId, uri, embedding);
                        break;
                    }

                    entities.add(uri);
                    break;
                }
            }
        });

        Map<String, List<Double>> embeddings = this.embeddingsDB.batchSelect(entities);
        embeddings.forEach((entity, embedding) -> {
            this.embeddingsIndex.clusterInsert(tableId, entity, embedding);
            this.embeddingsCache.put(entity, embedding);
        });
    }

    private void removeTableEmbeddings(String tableId)
    {
        this.embeddingsIndex.clusterRemove(tableId);
    }

    private List<Double> getTableEntityEmbedding(String table, String entity)
    {
        if (this.prefilter != null)
        {
            return this.embeddingsIndex.find(entity);
        }

        return this.embeddingsIndex.clusterGet(table, entity);
    }

    public void setCorpus(Set<String> tableFiles)
    {
        this.corpus = tableFiles;
        this.corpus = this.corpus.stream().map(t -> {
            String[] split = t.split("/");

            if (split.length == 0)
                return t;

            return split[split.length - 1];
        }).collect(Collectors.toSet());
    }

    private void prefilterSearchSpace(Table<String> query)
    {
        Iterator<Pair<String, Double>> res = this.prefilter.search(query).getResults();
        this.corpus.clear();

        while (res.hasNext())
        {
            this.corpus.add(res.next().getFirst());
        }

        if (this.useEmbeddings)
        {
            collectEmbeddings();
        }
    }

    private void collectEmbeddings()
    {
        List<String> entities = new ArrayList<>();
        int batchSize = 500;

        for (String table : this.corpus)
        {
            JsonTable jTable = TableParser.parse(new File(this.getEntityTableLink().getDirectory() + table));

            if (jTable == null || jTable.numDataRows == 0)
            {
                continue;
            }

            jTable.forEach(cell -> {
                for (String tableEntity : cell.links)
                {
                    String uri = getLinker().mapTo(tableEntity);

                    if (uri != null)
                    {
                        entities.add(uri);
                    }
                }
            });

            if (entities.size() >= batchSize)
            {
                Map<String, List<Double>> embeddings = this.embeddingsDB.batchSelect(entities);
                embeddings.forEach(this.embeddingsIndex::insert);
                entities.clear();
            }
        }

        Map<String, List<Double>> embeddings = this.embeddingsDB.batchSelect(entities);
        embeddings.forEach(this.embeddingsIndex::insert);
    }

    /**
     * Entry point for analogous search
     * @param query Input table query
     * @return Top-K ranked result container
     */
    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();

        if (this.prefilter != null)
        {
            prefilterSearchSpace(query);
            Logger.logNewLine(Logger.Level.INFO, "Pre-filtered corpus in " + this.prefilter.elapsedNanoSeconds() + "ns");
        }

        if (this.useEmbeddings)
        {
            setQueryEntityEmbeddings(query);
        }

        try
        {
            Logger.logNewLine(Logger.Level.INFO, "There are " + this.corpus.size() + " files to be processed.");
            ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
            List<Future<Pair<String, Double>>> parsed = new ArrayList<>(this.corpus.size());

            for (String table : this.corpus)
            {
                Future<Pair<String, Double>> future = threadPool.submit(() -> searchTable(query, table));
                parsed.add(future);
            }

            long done = 1, prev = 0, corpusSize = this.corpus.size();

            while (done != corpusSize)
            {
                done = parsed.stream().filter(Future::isDone).count();

                if (done - prev >= 100)
                {
                    Logger.log(Logger.Level.INFO, "Processed " + done + "/" + corpusSize + " files...");
                    prev = done;
                }
            }

            List<Pair<String, Double>> scores = new ArrayList<>();
            long parsedTables = parsed.stream().filter(f -> {
                try
                {
                    Pair<String, Double> tableScore = f.get();

                    if (tableScore != null)
                    {
                        scores.add(tableScore);
                        return true;
                    }

                    return false;
                }

                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e.getMessage());
                }
            }).count();

            this.elapsed = System.nanoTime() - start;
            this.parsedTables = parsedTables;
            Logger.logNewLine(Logger.Level.INFO, "A total of " + parsedTables + " tables were parsed.");
            Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + this.elapsed / 1e9 + " seconds\n");

            if (this.useEmbeddings)
            {
                Logger.logNewLine(Logger.Level.INFO, "A total of " + this.embeddingComparisons + " entity comparisons were made using embeddings.");
                Logger.logNewLine(Logger.Level.INFO, "A total of " + this.nonEmbeddingComparisons + " entity comparisons cannot be made due to lack of embeddings.");

                double percentage = (this.embeddingComparisons / ((double) this.nonEmbeddingComparisons + this.embeddingComparisons)) * 100;
                Logger.logNewLine(Logger.Level.INFO, percentage + "% of required entity comparisons were made using embeddings.\n");
                Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage successes: " + this.embeddingCoverageSuccesses);
                Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage failures: " + this.embeddingCoverageFails);
                Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage Success Rate: " + (double) this.embeddingCoverageSuccesses /
                        (this.embeddingCoverageSuccesses + this.embeddingCoverageFails));
                Logger.logNewLine(Logger.Level.INFO, "Query Entities with missing embedding coverage: " + this.queryEntitiesMissingCoverage + "\n");
            }

            return new Result(this.topK, scores);
        }

        catch (RuntimeException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    private Pair<String, Double> searchTable(Table<String> query, String table)
    {
        JsonTable jTable = TableParser.parse(new File(this.getEntityTableLink().getDirectory() + table));
        Stats.StatBuilder statBuilder = Stats.build();

        if (jTable == null || jTable.numDataRows == 0)
            return null;

        if (this.useEmbeddings && this.prefilter == null)
        {
            insertTableEntityEmbeddings(jTable, table);
        }

        List<List<Integer>> queryRowToColumnMappings = new ArrayList<>();  // If each query entity needs to map to only one column find the best mapping

        if (this.singleColumnPerQueryEntity)
        {
            queryRowToColumnMappings = getQueryToColumnMapping(query, jTable, table);
            List<List<String>> queryRowToColumnNames = new ArrayList<>(); // Log in the `statisticsMap` the column names aligned with each query row

            for (int queryRow = 0; queryRow < queryRowToColumnMappings.size(); queryRow++)
            {
                queryRowToColumnNames.add(new ArrayList<>());

                for (int entityId = 0; entityId < queryRowToColumnMappings.get(queryRow).size(); entityId++)
                {
                    int alignedColNum = queryRowToColumnMappings.get(queryRow).get(entityId);

                    if ((jTable.headers.size() > alignedColNum) && (alignedColNum >= 0))    // Ensure that `table` has headers that we can index them
                        queryRowToColumnNames.get(queryRow).add(jTable.headers.get(alignedColNum).text);
                }
            }

            statBuilder.tupleQueryAlignment(queryRowToColumnNames);
        }

        int numEntityMappedRows = 0;    // Number of rows in a table that have at least one cell mapping ot a known entity
        int queryRowsCount = query.rowCount();
        Table<List<Double>> scores = new DynamicTable<>();  // Each cell is a score of the corresponding query cell to the mapped cell in each table row

        for (int queryRowCounter = 0; queryRowCounter < queryRowsCount; queryRowCounter++)
        {
            int queryRowSize = query.getRow(queryRowCounter).size();
            List<List<Double>> queryRowsScores = new ArrayList<>(queryRowSize);

            for (int i = 0; i < queryRowSize; i++)
            {
                queryRowsScores.add(new ArrayList<>(jTable.rows.size()));
            }

            for (List<JsonTable.TableCell> tableRow : jTable.rows)
            {
                Map<Integer, String> columnToEntity = new HashMap<>();

                for (int tableColumn = 0; tableColumn < tableRow.size(); tableColumn++)
                {
                    for (String link : tableRow.get(tableColumn).links)
                    {
                        String uri = getLinker().mapTo(link);

                        if (uri != null)
                        {
                            columnToEntity.put(tableColumn, uri);
                            break;
                        }
                    }
                }

                if (columnToEntity.isEmpty())   // Compute similarity vectors only for rows that map to at least one entity
                    continue;

                numEntityMappedRows++;

                if (!this.useEmbeddings ||
                        hasEmbeddingCoverage(query.getRow(queryRowCounter), table, columnToEntity, queryRowToColumnMappings, queryRowCounter))
                {
                    for (int queryColumn = 0; queryColumn < queryRowSize; queryColumn++)
                    {
                        String queryEntity = query.getRow(queryRowCounter).get(queryColumn);
                        double bestSimScore = 0.0;

                        if (this.singleColumnPerQueryEntity)
                        {
                            int assignedColumn = queryRowToColumnMappings.get(queryRowCounter).get(queryColumn);

                            if (columnToEntity.containsKey(assignedColumn))
                            {
                                bestSimScore = entitySimilarityScore(queryEntity, columnToEntity.get(assignedColumn), table);
                            }
                        }

                        else
                        {
                            for (String rowEntity : columnToEntity.values()) // Loop over each entity in the table row
                            {
                                double simScore = entitySimilarityScore(queryEntity, rowEntity, table);
                                bestSimScore = Math.max(bestSimScore, simScore);
                            }
                        }

                        queryRowsScores.get(queryColumn).add(bestSimScore);
                    }
                }
            }

            scores.addRow(new Table.Row<>(queryRowsScores));
        }

        // Update Statistics
        statBuilder.entityMappedRows(numEntityMappedRows);
        statBuilder.fractionOfEntityMappedRows((double) numEntityMappedRows / jTable.numDataRows);
        Double score = aggregateTableSimilarities(query, scores, statBuilder);
        this.tableStats.put(table, statBuilder.finish());

        if (this.useEmbeddings && this.prefilter == null)
        {
            removeTableEmbeddings(table);
        }

        return new Pair<>(table, score);
    }

    /**
     * Initialize multi-dimensional array indexed by (tupleID, entityID, columnID) mapping to the
     * aggregated score for that query entity with respect to the column
     */
    private List<List<Integer>> getQueryToColumnMapping(Table<String> query, JsonTable table, String tableName)
    {
        List<List<List<Double>>> entityToColumnScore = new ArrayList<>();

        for (int row = 0; row < query.rowCount(); row++)
        {
            int rowSize = query.getRow(row).size();
            entityToColumnScore.add(new ArrayList<>(rowSize));

            for (int rowEntity = 0; rowEntity < rowSize; rowEntity++)
            {
                entityToColumnScore.get(row).add(new ArrayList<>(Collections.nCopies(table.numCols, 0.0)));
            }
        }

        // Loop over every cell in a table and populate 'entityToColumnScore'
        for (List<JsonTable.TableCell> row : table.rows)
        {
            int colCounter = 0;

            for (JsonTable.TableCell cell : row)
            {
                if(!cell.links.isEmpty())   // A cell value may map to multiple entities. Currently use the first one. TODO: Consider all of them?
                {
                    String curEntity = null;

                    for (String link : cell.links)
                    {
                        if (getLinker().mapTo(link) != null)    // Only consider links for which we have a known entity mapping
                        {
                            curEntity = getLinker().mapTo(link);
                            break;
                        }
                    }

                    if (curEntity != null)
                    {
                        for (int queryRow = 0; queryRow < query.rowCount(); queryRow++)    // Loop over each query tuple and each entity in a tuple and compute a score between the query entity and 'curEntity'
                        {
                            for (int queryEntityCounter = 0; queryEntityCounter < query.getRow(queryRow).size(); queryEntityCounter++)
                            {
                                String queryEntity = query.getRow(queryRow).get(queryEntityCounter);
                                Double score = entitySimilarityScore(queryEntity, curEntity, tableName);
                                entityToColumnScore.get(queryRow).get(queryEntityCounter).set(colCounter, entityToColumnScore.get(queryRow).get(queryEntityCounter).get(colCounter) + score);
                            }
                        }
                    }
                }

                colCounter++;
            }
        }

        List<List<Integer>> tupleToColumnMappings = getBestMatchFromScores(query, entityToColumnScore); // Find the best mapping between a query entity and a column for each query tuple.

        if (this.hungarianAlgorithmSameAlignmentAcrossTuples)
        {
            // TODO: Maybe perform a voting procedure instead of choosing to keep the alignment of the first query tuple

            for (int row = 1; row < tupleToColumnMappings.size(); row++)    // Modify tupleToColumnMappings so that the same column alignments are used across all query tuples
            {
                tupleToColumnMappings.set(row, tupleToColumnMappings.get(0));
            }
        }

        return tupleToColumnMappings;
    }

    /**
     * The similarity between two entities (this is a score between 0 and 1)
     *
     * By default the similarity is the jaccard similarity of the entity types corresponding to the entities.
     *
     * However if 'useEmbeddings' is specified and there exist embeddings for both entities
     * then use the angular distance between the two embedding vectors as the score.
     *
     * If 'usePretrainedEmbeddings' is not specified but 'adjustedJaccardSimilarity' is specified then
     * an adjusted Jaccard similarity between two entities is used where the similarity score is 1 only if the two entities are identical.
     * Otherwise a maximum similarity score is placed if the two entities are different
     * @param ent1 entity URI
     * @param ent2 entity URI
     * @return A score within [0, 1]
     */
    private double entitySimilarityScore(String ent1, String ent2, String table)
    {
        if (!this.useEmbeddings)
            return jaccardSimilarity(ent1, ent2);

        else if (entityExists(ent1, table) && entityExists(ent2, table))
            return cosineSimilarity(ent1, ent2, table);

        synchronized (this.lockStats)
        {
            this.nonEmbeddingComparisons++;
        }

        return 0.0;
    }

    private double jaccardSimilarity(String ent1, String ent2)
    {
        Set<Type> entTypes1 = new HashSet<>();
        Set<Type> entTypes2 = new HashSet<>();
        Id ent1Id = getLinker().kgUriLookup(ent1), ent2Id = getLinker().kgUriLookup(ent2);

        if (getEntityTable().contains(ent1Id))
            entTypes1 = new HashSet<>(getEntityTable().find(ent1Id).getTypes());

        if (getEntityTable().contains(ent2Id))
            entTypes2 = new HashSet<>(getEntityTable().find(ent2Id).getTypes());

        double jaccardScore = 0.0;

        if (this.weightedJaccard)   // Run weighted Jaccard Similarity
        {
            Set<Pair<Type, Double>> weights = entTypes1.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet());
            weights.addAll(entTypes2.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet()));
            weights = weights.stream().filter(p -> p.getSecond() >= 0).collect(Collectors.toSet());
            jaccardScore = JaccardSimilarity.make(entTypes1, entTypes2, weights).similarity();
        }

        else
            jaccardScore = JaccardSimilarity.make(entTypes1, entTypes2).similarity();

        if (this.adjustedJaccard)
            return ent1.equals(ent2) ? 1.0 : Math.min(0.95, jaccardScore);

        return jaccardScore;
    }

    private double cosineSimilarity(String ent1, String ent2, String table)
    {
        List<Double> ent1Embeddings = this.queryEntityEmbeddings.get(ent1),
                ent2Embeddings = this.queryEntityEmbeddings.get(ent2);

        if (ent1Embeddings == null)
        {
            ent1Embeddings = getTableEntityEmbedding(ent1, table);
        }

        if (ent2Embeddings == null)
        {
            ent2Embeddings = getTableEntityEmbedding(ent2, table);
        }

        if (ent1Embeddings == null || ent2Embeddings == null)
            return 0.0;

        double cosineSim = Utils.cosineSimilarity(ent1Embeddings, ent2Embeddings),
                simScore = 0.0;

        if (this.embeddingSimFunction == CosineSimilarityFunction.NORM_COS)
            simScore = (cosineSim + 1.0) / 2.0;

        else if (this.embeddingSimFunction == CosineSimilarityFunction.ABS_COS)
            simScore = Math.abs(cosineSim);

        else if (this.embeddingSimFunction == CosineSimilarityFunction.ANG_COS)
            simScore = 1 - Math.acos(cosineSim) / Math.PI;

        synchronized (this.lockStats)
        {
            this.embeddingComparisons++;
        }

        return simScore;
    }

    /**
     * Checks for existence of entity in database of entity embeddings
     * @param entity Entity to check
     * @return true if the entity exists in the embeddings database
     */
    private boolean entityExists(String entity, String table)
    {
        try
        {
            return this.queryEntityEmbeddings.containsKey(entity) || getTableEntityEmbedding(entity, table) != null;
        }

        catch (IllegalArgumentException exc)
        {
            return false;
        }
    }

    /**
     * Mapping of the matched columnIDs for each entity in each query tuple
     * Indexed by (tupleID, entityID) mapping to the columnID. If a columnID is -1 then that entity is not chosen for assignment
     * @param query Query table
     * @param entityToColumnScore Column score per entity
     * @return Best match from given scores
     */
    private List<List<Integer>> getBestMatchFromScores(Table<String> query, List<List<List<Double>>> entityToColumnScore)
    {
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();

        for (int row = 0; row < query.rowCount(); row++)
        {
            // 2-D array where each row is composed of the negative column relevance scores for a given entity in the query tuple
            // Taken from: https://stackoverflow.com/questions/10043209/convert-arraylist-into-2d-array-containing-varying-lengths-of-arrays
            double[][] scoresMatrix = entityToColumnScore.get(row).stream().map(u -> u.stream().mapToDouble(i -> -1 * i).toArray()).toArray(double[][]::new);

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

    /**
     * Given a list of the entities of a query tuple, the entities of a row in the table, the mapping
     * of the table columns to the query entities if any and the id of the query tuple; identify
     * if there exist pre-trained embeddings for each query entity and each matching row entity
     */
    private boolean hasEmbeddingCoverage(Table.Row<String> queryRow, String tableName,
                                         Map<Integer, String> columnToEntity, List<List<Integer>> tupleToColumnMappings,
                                         Integer queryRowIndex)
    {
        for (int i = 0; i < queryRow.size(); i++)   // Ensure that all query entities have an embedding
        {
            if (!entityExists(queryRow.get(i), tableName))
            {
                this.embeddingCoverageFails++;
                this.queryEntitiesMissingCoverage.add(queryRow.get(i));
                return false;
            }
        }

        // If `singleColumnPerQueryEntity` is true then ensure that all row entities that are
        // in the chosen columns (i.e. tupleToColumnMappings.get(queryTupleID) ) need to be mappable
        List<String> relevantRowEntities = new ArrayList<>();

        if (this.singleColumnPerQueryEntity)
        {
            for (int assignedColumn : tupleToColumnMappings.get(queryRowIndex))
            {
                if (columnToEntity.containsKey(assignedColumn))
                    relevantRowEntities.add(columnToEntity.get(assignedColumn));
            }
        }

        else    // All entities in `rowEntities` are relevant
            relevantRowEntities = new ArrayList<>(columnToEntity.values());

        for (String rowEnt : relevantRowEntities)   // Loop over all relevant row entities and ensure there is a pre-trained embedding mapping for each one
        {
            if (!entityExists(rowEnt, tableName))
            {
                this.embeddingCoverageFails++;
                return false;
            }
        }

        if (relevantRowEntities.isEmpty())
        {
            this.embeddingCoverageFails++;
            return false;
        }

        this.embeddingCoverageSuccesses++;
        return true;
    }

    /**
     * Aggregates scores into a single table score
     * @param query Query table
     * @param scores Table of query entity scores
     * @param statBuilder Statistics
     * @return Single score of table
     */
    private Double aggregateTableSimilarities(Table<String> query, Table<List<Double>> scores, Stats.StatBuilder statBuilder)
    {
        // Compute the weighted vector (i.e. considers IDF scores of query entities) for each query tuple
        Map<Integer, List<Double>> queryRowToWeightVector = new HashMap<>();

        for (int queryRow = 0; queryRow < query.rowCount(); queryRow++)
        {
            int rowSize = query.getRow(queryRow).size();
            List<Double> curRowIDFScores  = new ArrayList<>(rowSize);

            for (int column = 0; column < rowSize; column++)
            {
                Id entityId = getLinker().kgUriLookup(query.getRow(queryRow).get(column));
                curRowIDFScores.add(getEntityTable().find(entityId).getIDF());
            }

            queryRowToWeightVector.put(queryRow, Utils.normalizeVector(curRowIDFScores));
        }

        // Compute a score for the current file with respect to each query tuple
        // The score takes into account the weight vector associated with each tuple
        Map<Integer, Double> tupleIDToScore = new HashMap<>();
        List<List<Double>> queryRowVectors = new ArrayList<>();    // 2D List mapping each tupleID to the similarity scores chosen across the aligned columns

        for (int queryRow = 0; queryRow < query.rowCount(); queryRow++)
        {
            if (tableRowExists(scores.getRow(queryRow), l -> !l.isEmpty())) // Ensure that the current query row has at least one similarity vector with some row
            {
                List<Double> curQueryRowVec;

                if (this.useMaxSimilarityPerColumn)  // Use the maximum similarity score per column as the tuple vector
                    curQueryRowVec = Utils.getMaxPerColumnVector(scores.getRow(queryRow));

                else
                    curQueryRowVec = Utils.getAverageVector(scores.getRow(queryRow));

                List<Double> identityVector = new ArrayList<>(Collections.nCopies(curQueryRowVec.size(), 1.0));
                double score = 0.0;

                if (this.measure == SimilarityMeasure.COSINE)   // Note: Cosine similarity doesn't make sense if we are operating in a vector similarity space
                    score = Utils.cosineSimilarity(curQueryRowVec, identityVector);

                else if (this.measure == SimilarityMeasure.EUCLIDEAN)   // Perform weighted euclidean distance between the `curTupleVec` and `identity
                {
                    score = Utils.euclideanDistance(curQueryRowVec, identityVector, queryRowToWeightVector.get(queryRow));
                    score = 1 / (score + 1);    // Convert euclidean distance to similarity, high similarity (i.e. close to 1) means euclidean distance is small
                }

                tupleIDToScore.put(queryRow, score);
                queryRowVectors.add(curQueryRowVec);  // Update the tupleVectors array
            }

            else
                tupleIDToScore.put(queryRow, 0.0);
        }

        // TODO: Each tuple currently weighted equally. Maybe add extra weighting per tuple when taking average?
        if (!tupleIDToScore.isEmpty())  // Get a single score for the current filename that is averaged across all query tuple scores
        {
            List<Double> queryRowScores = new ArrayList<>(tupleIDToScore.values());
            statBuilder.queryRowScores(queryRowScores);
            statBuilder.queryRowVectors(queryRowVectors);
            return Utils.getAverageOfVector(queryRowScores);
        }

        return 0.0;
    }

    private <E> boolean tableRowExists(Table.Row<E> row, Predicate<E> function)
    {
        for (int i = 0; i < row.size(); i++)
        {
            if (function.test(row.get(i)))
                return true;
        }

        return false;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }

    private Set<String> distinctTables()
    {
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIter = getLinker().kgUriIds();

        while (entityIter.hasNext())
        {
            tables.addAll(getEntityTableLink().find(entityIter.next()));
        }

        return tables;
    }

    public int getEmbeddingComparisons()
    {
        return this.embeddingComparisons;
    }

    public int getNonEmbeddingComparisons()
    {
        return this.nonEmbeddingComparisons;
    }

    public int getEmbeddingCoverageSuccesses()
    {
        return this.embeddingCoverageSuccesses;
    }

    public int getEmbeddingCoverageFails()
    {
        return this.embeddingCoverageFails;
    }

    public Set<String> getQueryEntitiesMissingCoverage()
    {
        return this.queryEntitiesMissingCoverage;
    }

    public Map<String, Stats> getTableStats()
    {
        return this.tableStats;
    }

    public long getParsedTables()
    {
        return this.parsedTables;
    }
}
