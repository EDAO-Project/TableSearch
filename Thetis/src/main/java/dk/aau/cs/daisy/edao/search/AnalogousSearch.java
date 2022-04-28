package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.DBDriverBatch;
import dk.aau.cs.daisy.edao.loader.Stats;
import dk.aau.cs.daisy.edao.similarity.CosineSimilarity;
import dk.aau.cs.daisy.edao.similarity.JaccardSimilarity;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.system.Logger;
import dk.aau.cs.daisy.edao.tables.JsonTable;
import dk.aau.cs.daisy.edao.utilities.HungarianAlgorithm;
import dk.aau.cs.daisy.edao.utilities.Utils;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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
    private long elapsed = -1;
    private boolean useEmbeddings, singleColumnPerQueryEntity, weightedJaccard, useMaxSimilarityPerColumn;
    private CosineSimilarityFunction embeddingSimFunction;
    private SimilarityMeasure measure;
    private DBDriverBatch<List<Double>, String> embeddings;
    private Map<String, Stats> tableStats = new TreeMap<>();
    private final Object lock = new Object();
    private Set<String> corpus;

    public AnalogousSearch(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, int topK,
                           int threads, boolean useEmbeddings, CosineSimilarityFunction cosineFunction,
                           boolean singleColumnPerQueryEntity, boolean weightedJaccard, boolean useMaxSimilarityPerColumn,
                           SimilarityMeasure similarityMeasure, DBDriverBatch<List<Double>, String> embeddingStore)
    {
        super(linker, entityTable, entityTableLink);
        this.topK = topK;
        this.threads = threads;
        this.useEmbeddings = useEmbeddings;
        this.embeddingSimFunction = cosineFunction;
        this.singleColumnPerQueryEntity = singleColumnPerQueryEntity;
        this.weightedJaccard = weightedJaccard;
        this.useMaxSimilarityPerColumn = useMaxSimilarityPerColumn;
        this.measure = similarityMeasure;
        this.embeddings = embeddingStore;
        this.corpus = distinctTables();
    }

    public void setCorpus(Set<String> tableFiles)
    {
        this.corpus = tableFiles;
    }

    /**
     * Entrpy point for analogous search
     * @param query Input table query
     * @return Top-K ranked result container
     */
    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        long done = 1, prev = 0;
        ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
        List<Future<Pair<String, Double>>> parsed = new ArrayList<>(this.corpus.size());

        for (String table : this.corpus)
        {
            Future<Pair<String, Double>> f = threadPool.submit(() -> new Pair<>(table, searchTable(query, table)));
            parsed.add(f);
        }

        while (done != this.corpus.size())
        {
            done = parsed.stream().filter(Future::isDone).count();

            if (done - prev >= 100)
            {
                Logger.log(Logger.Level.INFO, "Processed " + done + "/" + this.corpus.size() + " files...");
                prev = done;
            }
        }

        List<Pair<String, Double>> scores = new ArrayList<>();
        long parsedTables = parsed.stream().filter(f -> {
            try
            {
                Pair<String, Double> score = f.get();

                if (score.getSecond() != null)
                {
                    scores.add(score);
                    return true;
                }

                return false;
            }

            catch (InterruptedException | ExecutionException e)
            {
                Logger.logNewLine(Logger.Level.ERROR, "Exception when scoring table");
                throw new RuntimeException(e.getMessage());
            }
        }).count();

        this.elapsed = System.nanoTime() - start;

        Logger.logNewLine(Logger.Level.INFO, "A total of " + parsedTables + " tables were parsed.");
        Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + this.elapsed / 1e9 + " seconds\n");

        if (this.useEmbeddings)
        {
            Logger.logNewLine(Logger.Level.INFO, "A total of " + this.embeddingComparisons + " entity comparisons were made using embeddings.");
            Logger.logNewLine(Logger.Level.INFO, "A total of " + this.nonEmbeddingComparisons + " entity comparisons cannot be made due to lack of embeddings.");

            double percentage = (this.embeddingComparisons / ((double) this.nonEmbeddingComparisons + this.embeddingComparisons)) * 100;
            Logger.logNewLine(Logger.Level.INFO, percentage + "% of required entity comparisons were made using embeddings.\n");
            Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage successes: " + embeddingCoverageSuccesses);
            Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage failures: " + embeddingCoverageFails);
            Logger.logNewLine(Logger.Level.INFO, "Embedding Coverage Success Rate: " + (double) embeddingCoverageSuccesses / (embeddingCoverageSuccesses + embeddingCoverageFails));
            Logger.logNewLine(Logger.Level.INFO, "Query Entities with missing embedding coverage: " + queryEntitiesMissingCoverage + "\n");
        }

        return new Result(this.topK, scores);
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }

    private Set<String> distinctTables()
    {
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIter = getLinker().getDictionary().elements().asIterator();

        while (entityIter.hasNext())
        {
            tables.addAll(getEntityTableLink().find(entityIter.next()));
        }

        return tables;
    }

    private Double searchTable(Table<String> query, String table)
    {
        JsonTable jTable = TableParser.parse(new File(table));
        Stats.StatBuilder statBuilder = Stats.build();

        if (jTable == null || jTable.numDataRows == 0)
            return null;

        List<List<Integer>> queryRowToColumnMappings = new ArrayList<>();
        Thread statsThread = null;

        if (this.singleColumnPerQueryEntity)
        {
            queryRowToColumnMappings = getQueryToColumnMapping(query, jTable);
            List<List<Integer>> queryRowToColumnMappingsCopy = queryRowToColumnMappings;
            statsThread = new Thread(() -> statBuilder.tupleQueryAlignment(computeQueryTupleAlignment(queryRowToColumnMappingsCopy, jTable)));
            statsThread.start();
        }

        Map<Integer, Map<Integer, List<Double>>> tableRowSimilarities = new HashMap<>(jTable.numDataRows);
        int numEntityMappedRows = 0, rowId = 0;

        for (List<JsonTable.TableCell> row : jTable.rows)
        {
            Map<Integer, String> columnToEntity = columnToEntityMapping(row);

            // Compute similarity vectors only for rows that map to at least one entity
            if (!columnToEntity.isEmpty())
            {
                numEntityMappedRows++;
                tableRowSimilarities.put(rowId,
                        computeQueryRowToTableRowSimilarities(query, columnToEntity, queryRowToColumnMappings));
            }

            rowId++;
        }

        try
        {
            statBuilder.entityMappedRows(numEntityMappedRows);
            statBuilder.fractionOfEntityMappedRows((double) numEntityMappedRows / jTable.numDataRows);

            if (this.singleColumnPerQueryEntity)
                statsThread.join();
        }

        catch (InterruptedException ignored) {}

        double tableScore = aggregateTableSimilarities(query, tableRowSimilarities, statBuilder);
        this.tableStats.put(table, statBuilder.finish());
        return tableScore;
    }

    private List<List<String>> computeQueryTupleAlignment(List<List<Integer>> queryRowToColumnMappings, JsonTable table)
    {
        List<List<String>> queryTupleToColumnNames = new ArrayList<>();

        for (int queryRow = 0; queryRow < queryRowToColumnMappings.size(); queryRow++)
        {
            queryTupleToColumnNames.add(new ArrayList<>());

            for (int rowColumn = 0; rowColumn < queryRowToColumnMappings.get(queryRow).size(); rowColumn++)
            {
                int alignedColNum = queryRowToColumnMappings.get(queryRow).get(rowColumn);

                // Ensure that `jTable` has headers that we can index them
                if ((table.headers.size() > alignedColNum) && (alignedColNum >= 0))
                    queryTupleToColumnNames.get(queryRow).add(table.headers.get(alignedColNum).text);
            }
        }

        return queryTupleToColumnNames;
    }

    private Map<Integer, String> columnToEntityMapping(List<JsonTable.TableCell> row)
    {
        Map<Integer, String> columnToEntity = new HashMap<>();

        for (int colId = 0; colId < row.size(); colId++)
        {
            if (!row.get(colId).links.isEmpty())
            {
                // A cell value may map to multiple entities. Currently, use the first one.
                for (String link : row.get(colId).links)
                {
                    String entityUri = getLinker().mapTo(link);

                    // Only consider links for which we have a known entity mapping
                    if (entityUri != null)
                    {
                        columnToEntity.put(colId, entityUri);
                        break;
                    }
                }
            }
        }

        return columnToEntity;
    }

    private boolean hasEmbeddingCoverage(Table.Row<String> queryRow, Map<Integer, String> columnToEntity,
                                         List<List<Integer>> tupleToColumnMappings, int queryRowIndex)
    {
        // Ensure that all query entities have an embedding
        for (int i = 0; i < queryRow.size(); i++)
        {
            if (!entityExists(queryRow.get(i)))
            {
                this.embeddingCoverageFails++;
                this.queryEntitiesMissingCoverage.add(queryRow.get(i));
                return false;
            }
        }

        // If `singleColumnPerQueryEntity` is true, then ensure that all row entities that are
        // in the chosen columns (i.e. tupleToColumnMappings.get(queryTupleID)) need to be mappable
        List<String> relevanRowEntities = new ArrayList<>();

        if (this.singleColumnPerQueryEntity)
        {
            for (int assignedColumn : tupleToColumnMappings.get(queryRowIndex))
            {
                if (columnToEntity.containsKey(assignedColumn)) {
                    relevanRowEntities.add(columnToEntity.get(assignedColumn));
                }
            }
        }

        else    // All entities in `rowEntities` are relevant
            relevanRowEntities = new ArrayList<>(columnToEntity.values());

        // Loop over all relevant row entities and ensure there is a pre-trained embedding mapping for each one
        for (String rowEnt : relevanRowEntities)
        {
            if (!entityExists(rowEnt))
            {
                this.embeddingCoverageFails++;
                return false;
            }
        }

        if (relevanRowEntities.isEmpty())
        {
            this.embeddingCoverageFails++;
            return false;
        }

        this.embeddingCoverageSuccesses++;
        return true;
    }

    // Initialize multi-dimensional array indexed by
    // (tupleID/query row index, entityID/query column index, columnID/table column index) mapping to the
    // aggregated score for that query entity with respect to the column
    public List<List<Integer>> getQueryToColumnMapping(Table<String> query, JsonTable table)
    {
        List<List<List<Double>>> entityToColumnScore = new ArrayList<>();

        for (int queryRow = 0; queryRow < query.rowCount(); queryRow++)
        {
            entityToColumnScore.add(new ArrayList<>(query.getRow(queryRow).size()));

            for (int queryColumn = 0; queryColumn < query.getRow(queryRow).size(); queryColumn++)
            {
                entityToColumnScore.get(queryRow).add(new ArrayList<>(Collections.nCopies(table.numCols, 0.0)));
            }
        }

        // Loop over every cell in a table and populate 'entityToColumnScore'
        for (List<JsonTable.TableCell> row : table.rows)
        {
            int column = 0;

            for (JsonTable.TableCell cell : row)
            {
                if (!cell.links.isEmpty())
                {
                    String curEntity = null;

                    // A cell value may map to multiple entities. Currently use the first one. TODO: Consider all of them?
                    for (String link : cell.links)
                    {
                        String entityUri = getLinker().mapTo(link);

                        // Only consider links for which we have a known entity mapping
                        if (entityUri != null)
                        {
                            curEntity = entityUri;
                            break;
                        }
                    }
                    if (curEntity != null)
                    {
                        // Loop over each query tuple and each entity in a tuple and compute a score between the query entity and 'curEntity'
                        for (int queryRow = 0; queryRow < query.rowCount(); queryRow++)
                        {
                            for (int queryColumn = 0; queryColumn < query.getRow(queryRow).size(); queryColumn++)
                            {
                                String queryEntity = query.getRow(queryRow).get(queryColumn);
                                Double score = entitySimilarityScore(queryEntity, curEntity);
                                entityToColumnScore.get(queryRow).get(queryColumn).set(column, entityToColumnScore.get(queryRow).get(queryColumn).get(column) + score);
                            }
                        }
                    }
                }

                column++;
            }
        }

        return getBestMatchFromScores(query, entityToColumnScore); // Find the best mapping between a query entity and a column for each query tuple.
    }

    private double entitySimilarityScore(String ent1, String ent2)
    {
        // Jaccard similarity
        if (!this.useEmbeddings)
            return jaccardSimilarity(ent1, ent2);

        // Embeddings cosine similarity: Check if there are embeddings for both entities
        if (entityExists(ent1) && entityExists(ent2))
        {
            // Compute the appropriate score based on the specified EmbeddingSimFunction
            double cosine = CosineSimilarity.make(this.embeddings.select(ent1), this.embeddings.select(ent2)).similarity();
            double simScore = 0.0;

            if (this.embeddingSimFunction == CosineSimilarityFunction.NORM_COS)
                simScore = (cosine + 1.0) / 2.0;

            else if (this.embeddingSimFunction == CosineSimilarityFunction.ABS_COS)
                simScore = Math.abs(cosine);

            else if (this.embeddingSimFunction == CosineSimilarityFunction.ANG_COS)
                simScore = 1 - Math.acos(cosine) / Math.PI;

            synchronized (this.lock)
            {
                this.embeddingComparisons += 1;
            }

            return simScore;
        }

        // No mapped pre-trained embeddings found for both `ent1` and `ent2` so we skip this comparison and return 0
        synchronized (this.lock)
        {
            this.nonEmbeddingComparisons += 1;
        }

        return 0.0;
    }

    private double jaccardSimilarity(String ent1, String ent2)
    {
        double jaccard;
        Id ent1Id = getLinker().getDictionary().get(ent1), ent2Id = getLinker().getDictionary().get(ent2);
        Set<Type> ent1Types = ent1Id != null ? Set.copyOf(getEntityTable().find(ent1Id).getTypes()) : null,
                ent2Types = ent2Id != null ? Set.copyOf(getEntityTable().find(ent2Id).getTypes()) : null;

        if (ent1Types == null || ent2Types == null)
            return -1;

        if (this.weightedJaccard)
        {
            Set<Pair<Type, Double>> weights = ent1Types.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet());
            weights.addAll(ent2Types.stream().map(t -> new Pair<>(t, t.getIdf())).collect(Collectors.toSet()));
            jaccard = JaccardSimilarity.make(ent1Types, ent2Types, weights).similarity();
        }

        else
            jaccard = JaccardSimilarity.make(ent1Types, ent2Types).similarity();

        // The two entities compared are equal so return a score of 1.0
        if (ent1.equals(ent2))
            return 1.0;

        else    // Adjusted Jaccard: The two entities compared are different. Impose a maximum possible score less than 1.0
            return Math.min(jaccard, 0.95);
    }

    private Map<Integer, List<Double>> computeQueryRowToTableRowSimilarities(Table<String> query, Map<Integer, String> columnToEntity,
                                                                             List<List<Integer>> queryRowToColumnMappings)
    {
        int rowCount = query.rowCount();
        Map<Integer, List<Double>> sims = new HashMap<>(rowCount);

        for (int queryRow = 0; queryRow < rowCount; queryRow++)
        {
            if (!this.useEmbeddings || hasEmbeddingCoverage(query.getRow(queryRow), columnToEntity, queryRowToColumnMappings, queryRow))
                sims.put(queryRow,
                        computeQueryRowToTableRowSimilarity(query.getRow(queryRow), queryRow, columnToEntity, queryRowToColumnMappings));

            else
            {
                // TODO: Track how many (tupleID, rowID) pairs were skipped due to lack of embedding mappings
            }
        }

        return sims;
    }

    private List<Double> computeQueryRowToTableRowSimilarity(Table.Row<String> queryRow, int rowIndex, Map<Integer, String> columnToEntity,
                                                               List<List<Integer>> queryRowToColumnMappings)
    {
        int queryRowWidth = queryRow.size();
        List<Double> maxQueryRow = new ArrayList<>(Collections.nCopies(queryRowWidth, 0.0));

        for (int queryRowColumn = 0; queryRowColumn < queryRowWidth; queryRowColumn++)
        {
            String queryEntity = queryRow.get(queryRowColumn);
            double bestSimScore = 0.0;

            if (this.singleColumnPerQueryEntity)
            {
                int assignedColumn = queryRowToColumnMappings.get(rowIndex).get(queryRowColumn);

                if (columnToEntity.containsKey(assignedColumn))
                    bestSimScore = entitySimilarityScore(queryEntity, columnToEntity.get(assignedColumn));
            }

            else
            {
                for (String rowEntity : columnToEntity.values())
                {
                    double simScore = entitySimilarityScore(queryEntity, rowEntity);

                    if (simScore > bestSimScore)
                        bestSimScore = simScore;
                }
            }

            maxQueryRow.set(queryRowColumn, bestSimScore);
        }

        return maxQueryRow;
    }

    private boolean entityExists(String entity)
    {
        try
        {
            return this.embeddings.select(entity) != null;
        }

        catch (IllegalArgumentException exc)
        {
            return false;
        }
    }

    // Mapping of the matched columnIDs for each entity in each query tuple
    // Indexed by (tupleID, entityID) mapping to the columnID. If a columnID is -1 then that entity is not chosen for assignment
    private List<List<Integer>> getBestMatchFromScores(Table<String> query, List<List<List<Double>>> entityToColumnScore)
    {
        List<List<Integer>> tupleToColumnMappings = new ArrayList<>();

        for (int row = 0; row < query.rowCount(); row++) {
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

    private Double aggregateTableSimilarities(Table<String> query, Map<Integer, Map<Integer, List<Double>>> tableRowSimilarities,
                                              Stats.StatBuilder statBuilder)
    {
        Map<Integer, List<List<Double>>> queryRowToSimVectors = toQueryToSimilarities(tableRowSimilarities);
        Map<Integer, List<Double>> queryRowToWeightVector = computeQueryRowWeights(query);  // Compute the weighted vector (i.e. considers IDF scores of query entities) for each query tuple
        List<List<Double>> queryRowVectors = new ArrayList<>();
        Map<Integer, Double> queryRowToScore = new HashMap<>();
        int queryRows = query.rowCount();

        for (int row = 0; row < queryRows; row++)
        {
            if (tableRowSimilarities.size() > 0)
            {
                if (queryRowToSimVectors.containsKey(row))
                {
                    List<Double> curRowVec;

                    if (this.useMaxSimilarityPerColumn)
                        curRowVec = Utils.getMaxPerColumnVector(queryRowToSimVectors.get(row));

                    else
                        curRowVec = Utils.getAverageVector(queryRowToSimVectors.get(row));

                    List<Double> identityVector = new ArrayList<>(Collections.nCopies(curRowVec.size(), 1.0));
                    double score = 0.0;

                    if (this.measure == SimilarityMeasure.COSINE)
                        score = Utils.cosineSimilarity(curRowVec, identityVector);

                    else if (this.measure == SimilarityMeasure.EUCLIDEAN)
                    {
                        score = Utils.euclideanDistance(curRowVec, identityVector, queryRowToWeightVector.get(row));
                        score = 1 / (score + 1);
                    }

                    queryRowToScore.put(row, score);
                    queryRowVectors.add(curRowVec);
                }
            }

            else
                queryRowToScore.put(row, 0.0);
        }

        if (!queryRowToScore.isEmpty())
        {
            List<Double> queryRowScores = new ArrayList<>(queryRowToScore.values());
            statBuilder.queryRowScores(queryRowScores);
            statBuilder.queryRowVectors(queryRowVectors);
            return Utils.getAverageOfVector(queryRowScores);
        }

        return 0.0;
    }

    // Map each query row to a list of all similarity vectors for the given table map
    private static Map<Integer, List<List<Double>>> toQueryToSimilarities(Map<Integer, Map<Integer, List<Double>>> tableRowSimilarities)
    {
        Map<Integer, List<List<Double>>> queryRowToSimVectors = new HashMap<>();

        for (int row : tableRowSimilarities.keySet())
        {
            for (int queryRow : tableRowSimilarities.get(row).keySet())
            {
                List<Double> currSimVector = tableRowSimilarities.get(row).get(queryRow);

                if (!queryRowToSimVectors.containsKey(queryRow))
                {
                    List<List<Double>> simVectors = new ArrayList<>();
                    simVectors.add(currSimVector);
                    queryRowToSimVectors.put(queryRow, simVectors);
                }

                else
                    queryRowToSimVectors.get(queryRow).add(currSimVector);
            }
        }

        return queryRowToSimVectors;
    }

    private Map<Integer, List<Double>> computeQueryRowWeights(Table<String> query)
    {
        Map<Integer, List<Double>> queryRowToWeightVector = new HashMap<>();
        int queryRowCount = query.rowCount();

        for (int queryRow = 0; queryRow < queryRowCount; queryRow++)
        {
            List<Double> curRowIDFScores = new ArrayList<>();
            int rowSize = query.getRow(queryRow).size();

            for (int column = 0; column < rowSize; column++)
            {
                Id entityId = getLinker().getDictionary().get(query.getRow(queryRow).get(column));
                double entityIdf = getEntityTable().find(entityId).getIDF();
                curRowIDFScores.add(entityIdf);
            }

            queryRowToWeightVector.put(queryRow, Utils.normalizeVector(curRowIDFScores));
        }

        return queryRowToWeightVector;
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
}
