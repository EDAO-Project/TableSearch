package com.thetis.loader;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.thetis.store.*;
import com.thetis.store.hnsw.HNSW;
import com.thetis.commands.parser.TableParser;
import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.structures.Pair;
import com.thetis.structures.PairNonComparable;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Type;
import com.thetis.structures.table.DynamicTable;
import com.thetis.structures.table.Table;
import com.thetis.system.Configuration;
import com.thetis.system.Logger;
import com.thetis.tables.JsonTable;
import com.thetis.utilities.Utils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Main class for building indexes and serializing them on disk
 */
public class IndexWriter implements IndexIO
{
    private List<Path> files;
    private File outputPath;
    private int threads;
    protected AtomicLong loadedTables = new AtomicLong(0);
    protected AtomicInteger cellsWithLinks = new AtomicInteger(0), tableStatsCollected = new AtomicInteger(0);
    protected final Object lock = new Object(), incrementLock = new Object();
    private long elapsed = -1;
    private Map<Integer, Integer> cellToNumLinksFrequency = Collections.synchronizedMap(new HashMap<>());
    private Map<Integer, Integer> linkToNumEntitiesFrequency = Collections.synchronizedMap(new HashMap<>());
    protected Neo4jEndpoint neo4j;
    protected Linker entityLinker;
    protected SynchronizedLinker<String, String> linker;
    protected SynchronizedIndex<Id, Entity> entityTable;
    protected SynchronizedIndex<Id, List<String>> entityTableLink;
    protected SynchronizedIndex<Id, List<Double>> embeddingsIdx;
    protected final SynchronizedIndex<String, Set<String>> hnsw;
    protected DBDriverBatch<List<Double>, String> embeddingsDB;
    protected BloomFilter<String> filter = BloomFilter.create(
            Funnels.stringFunnel(Charset.defaultCharset()),
            5_000_000,
            0.01);
    private final Map<String, Stats> tableStats = new TreeMap<>();
    private final Set<PairNonComparable<String, Table<String>>> tableEntities = Collections.synchronizedSet(new HashSet<>());
    protected List<String> disallowedEntityTypes;
    protected static final int HNSW_K = 10000;

    public IndexWriter(List<Path> files, File outputDir, Linker entityLinker, Neo4jEndpoint neo4j, int threads,
                       DBDriverBatch<List<Double>, String> embeddingStore, String wikiPrefix, String uriPrefix, String ... disallowedEntityTypes)
    {
        if (!outputDir.exists())
        {
            outputDir.mkdirs();
        }

        else if (!outputDir.isDirectory())
        {
            throw new IllegalArgumentException("Output directory '" + outputDir + "' is not a directory");
        }

        else if (files.isEmpty())
        {
            throw new IllegalArgumentException("Missing files to load");
        }

        this.files = files;
        this.outputPath = outputDir;
        this.neo4j = neo4j;
        this.entityLinker = entityLinker;
        this.threads = threads;
        this.embeddingsDB = embeddingStore;
        this.linker = SynchronizedLinker.wrap(new EntityLinking(wikiPrefix, uriPrefix));
        this.disallowedEntityTypes = Arrays.asList(disallowedEntityTypes);
        this.entityTable = SynchronizedIndex.wrap(new EntityTable());
        this.embeddingsIdx = SynchronizedIndex.wrap(new EmbeddingsIndex<>());
        this.entityTableLink = SynchronizedIndex.wrap(new EntityTableLink());
        this.hnsw = SynchronizedIndex.wrap(new HNSW((entity -> this.embeddingsDB.select(entity.replace("'", "''"))), Configuration.getEmbeddingsDimension(),
                neo4j.getNumNodes(), HNSW_K, getEntityLinker(), getEntityTableLinker(), this.outputPath + "/" + Configuration.getHNSWFile()));
        ((EntityTableLink) this.entityTableLink.getIndex()).setDirectory(files.get(0).toFile().getParent() + "/");
    }

    /**
     * Loading of tables to disk
     */
    @Override
    public void performIO() throws IOException
    {
        if (this.loadedTables.get() > 0)
        {
            throw new RuntimeException("Loading has already complete");
        }

        int size = this.files.size();
        ExecutorService threadPool = Executors.newFixedThreadPool(this.threads);
        List<Future<Boolean>> tasks = new ArrayList<>(size);
        long startTime = System.nanoTime(), prev = 0;

        for (int i = 0; i < size; i++)
        {
            final int index = i;
            Future<Boolean> task = threadPool.submit(() -> load(this.files.get(index)));
            tasks.add(task);
        }

        while (this.loadedTables.get() < size)
        {
            synchronized (this.incrementLock)
            {
                this.loadedTables.set(tasks.stream().filter(Future::isDone).count());
            }

            if (this.loadedTables.get() - prev >= 100)
            {
                Logger.log(Logger.Level.INFO, "Processed " + this.loadedTables.get() + "/" + size + " files...");
                prev = this.loadedTables.get();
            }
        }

        tasks.forEach(t ->
        {
            try
            {
                t.get();
            }

            catch (InterruptedException | ExecutionException ignored) {}
        });
        Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
        loadIDFs();
        Logger.logNewLine(Logger.Level.INFO, "Writing indexes and stats on disk...");
        writeStats();
        this.tableStats.clear();    // Clean up to save space before writing index objects to disk
        flushToDisk();

        this.elapsed = System.nanoTime() - startTime;
        Logger.log(Logger.Level.INFO, "Done");
        Logger.logNewLine(Logger.Level.INFO, "A total of " + this.loadedTables.get() + " tables were loaded");
        Logger.logNewLine(Logger.Level.INFO, "Elapsed time: " + this.elapsed / (1e9) + " seconds");
        Logger.logNewLine(Logger.Level.INFO, "Computing IDF weights...");
    }

    private boolean load(Path tablePath)
    {
        JsonTable table = TableParser.parse(tablePath);

        if (table == null ||  table._id == null || table.rows == null)
        {
            return false;
        }

        String tableName = tablePath.getFileName().toString();
        Map<Pair<Integer, Integer>, List<String>> entityMatches = new HashMap<>();  // Maps a cell specified by RowNumber, ColumnNumber to the list of entities it matches to
        Table<String> parsedTable = new DynamicTable<>();   // The set of entities corresponding to this filename/table
        int row = 0;

        for (List<JsonTable.TableCell> tableRow : table.rows)
        {
            int column = 0;
            List<String> parsedRow = new ArrayList<>();

            for (JsonTable.TableCell cell : tableRow)
            {
                if (!cell.links.isEmpty())
                {
                    this.cellsWithLinks.incrementAndGet();
                    this.cellToNumLinksFrequency.merge(cell.links.size(), 1, Integer::sum);
                    List<String> matchesUris = new ArrayList<>();

                    for (String link : cell.links)
                    {
                        if (this.linker.mapTo(link) != null)   // Check if we had already searched for it
                        {
                            matchesUris.add(this.linker.mapTo(link));
                        }

                        else
                        {
                            String entity = this.entityLinker.link(link.replace("http://www.", "http://en."));

                            if (entity != null)
                            {
                                synchronized (this.lock)
                                {
                                    List<String> entityTypes = this.neo4j.searchTypes(entity);
                                    List<String> entityPredicates = this.neo4j.searchPredicates(entity);
                                    matchesUris.add(entity);
                                    this.linker.addMapping(link, entity);
                                    this.linkToNumEntitiesFrequency.merge(1, 1, Integer::sum);

                                    for (String type : this.disallowedEntityTypes)
                                    {
                                        entityTypes.remove(type);
                                    }

                                    Id entityId = ((EntityLinking) this.linker.getLinker()).kgUriLookup(entity);
                                    List<Double> embeddings = this.embeddingsDB.select(entity.replace("'", "''"));
                                    this.entityTable.insert(entityId,
                                        new Entity(entity, entityTypes.stream().map(Type::new).collect(Collectors.toList()), entityPredicates));
                                    this.hnsw.insert(entity, Collections.emptySet());

                                    if (embeddings != null)
                                    {
                                        this.embeddingsIdx.insert(entityId, embeddings);
                                    }
                                }
                            }
                        }

                        if (this.linker.mapTo(link) != null)
                        {
                            String entity = this.linker.mapTo(link);
                            Id entityId = ((EntityLinking) this.linker.getLinker()).kgUriLookup(entity);
                            Pair<Integer, Integer> location = new Pair<>(row, column);

                            synchronized (this.lock)
                            {
                                ((EntityTableLink) this.entityTableLink.getIndex()).
                                        addLocation(entityId, tableName, List.of(location));
                            }
                        }
                    }

                    if (!matchesUris.isEmpty())
                    {
                        for (String entity : matchesUris)
                        {
                            synchronized (this.lock)
                            {
                                this.filter.put(entity);
                                parsedRow.add(entity);
                            }
                        }

                        entityMatches.put(new Pair<>(row, column), matchesUris);
                    }
                }

                column++;
            }

            parsedTable.addRow(new Table.Row<>(parsedRow));
            row++;
        }

        synchronized (this.lock)
        {
            this.tableEntities.add(new PairNonComparable<>(tableName, parsedTable));
            saveStats(table, FilenameUtils.removeExtension(tableName), parsedTable, entityMatches);
        }

        return true;
    }

    private void saveStats(JsonTable jTable, String tableFileName, Table<String> table, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        Stats stats = collectStats(jTable, tableFileName, table, entityMatches);
        this.tableStats.put(tableFileName, stats);
    }

    private Stats collectStats(JsonTable jTable, String tableFileName, Table<String> table, Map<Pair<Integer, Integer>, List<String>> entityMatches)
    {
        List<Integer> numEntitiesPerRow = new ArrayList<>(Collections.nCopies(jTable.numDataRows, 0));
        List<Integer> numEntitiesPerCol = new ArrayList<>(Collections.nCopies(jTable.numCols, 0));
        List<Integer> numCellToEntityMatchesPerCol = new ArrayList<>(Collections.nCopies(jTable.numCols, 0));
        List<Boolean> tableColumnsIsNumeric = new ArrayList<>(Collections.nCopies(jTable.numCols, false));
        long numCellToEntityMatches = 0L; // Specifies the total number (bag semantics) of entities all cells map to
        int entityCount = 0, rows = table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            for (int column = 0; column < table.getRow(row).size(); column++)
            {
                entityCount++;
                Id entityId = ((EntityLinking) this.linker.getLinker()).kgUriLookup(table.getRow(row).get(column));

                if (entityId == null)
                {
                    continue;
                }

                List<Pair<Integer, Integer>> locations =
                        ((EntityTableLink) this.entityTableLink.getIndex()).getLocations(entityId, tableFileName);

                if (locations != null)
                {
                    for (Pair<Integer, Integer> location : locations)
                    {
                        numEntitiesPerRow.set(location.getFirst(), numEntitiesPerRow.get(location.getFirst()) + 1);
                        numEntitiesPerCol.set(location.getSecond(), numEntitiesPerCol.get(location.getSecond()) + 1);
                        numCellToEntityMatches++;
                    }
                }
            }
        }

        for (Pair<Integer, Integer> position : entityMatches.keySet())
        {
            Integer colId = position.getSecond();
            numCellToEntityMatchesPerCol.set(colId, numCellToEntityMatchesPerCol.get(colId) + 1);
        }

        if (jTable.numNumericCols == jTable.numCols)
        {
            tableColumnsIsNumeric = new ArrayList<>(Collections.nCopies(jTable.numCols, true));
        }

        else
        {
            int colId = 0;

            for (JsonTable.TableCell cell : jTable.rows.get(0))
            {
                if (cell.isNumeric)
                {
                    tableColumnsIsNumeric.set(colId, true);
                }

                colId++;
            }
        }

        this.tableStatsCollected.incrementAndGet();
        return Stats.build()
                .rows(jTable.numDataRows)
                .columns(jTable.numCols)
                .cells(jTable.numDataRows * jTable.numCols)
                .entities(entityCount)
                .mappedCells(entityMatches.size())
                .entitiesPerRow(numEntitiesPerRow)
                .entitiesPerColumn(numEntitiesPerCol)
                .cellToEntityMatches(numCellToEntityMatches)
                .cellToEntityMatchesPerCol(numCellToEntityMatchesPerCol)
                .numericTableColumns(tableColumnsIsNumeric)
                .finish();
    }

    private void writeStats()
    {
        File statDir = new File(this.outputPath + "/statistics/");

        if (!statDir.exists())
        {
            statDir.mkdir();
        }

        try
        {
            FileWriter writer = new FileWriter(statDir + "/" + Configuration.getWikiLinkToEntitiesFrequencyFile());
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(this.linkToNumEntitiesFrequency, writer);
            writer.flush();
            writer.close();

            writer = new FileWriter(statDir + "/" + Configuration.getCellToNumLinksFrequencyFile());
            gson.toJson(this.cellToNumLinksFrequency, writer);
            writer.flush();
            writer.close();

            writer = new FileWriter(statDir + "/" + Configuration.getTableStatsFile());
            gson.toJson(this.tableStats, writer);
            writer.flush();
            writer.close();
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    protected void loadIDFs()
    {
        loadEntityIDFs();
        loadTypeIDFs();
    }

    private void loadEntityIDFs()
    {
        Iterator<Id> idIter = ((EntityLinking) this.linker.getLinker()).kgUriIds();

        while (idIter.hasNext())
        {
            Id entityId = idIter.next();
            double idf = Math.log10((double) this.loadedTables.get() / this.entityTableLink.find(entityId).size()) + 1;
            this.entityTable.find(entityId).setIDF(idf);
        }
    }

    private void loadTypeIDFs()
    {
        Map<Type, Integer> entityTypeFrequency = new HashMap<>();
        Iterator<Id> idIterator = ((EntityLinking) this.linker.getLinker()).kgUriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();
            List<Type> entityTypes = this.entityTable.find(id).getTypes();

            for (Type t : entityTypes)
            {
                if (entityTypeFrequency.containsKey(t))
                {
                    entityTypeFrequency.put(t, entityTypeFrequency.get(t) + 1);
                }

                else
                {
                    entityTypeFrequency.put(t, 1);
                }
            }
        }

        long totalEntityCount = this.entityTable.size();
        idIterator = ((EntityLinking) this.linker.getLinker()).kgUriIds();

        while (idIterator.hasNext())
        {
            Id id = idIterator.next();
            this.entityTable.find(id).getTypes().forEach(t -> {
                if (entityTypeFrequency.containsKey(t))
                {
                    double idf = Utils.log2((double) totalEntityCount / entityTypeFrequency.get(t));
                    t.setIdf(idf);
                }
            });
        }
    }

    protected void flushToDisk() throws IOException
    {
        // Entity linker
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityLinkerFile()));
        outputStream.writeObject(this.linker.getLinker());
        outputStream.flush();
        outputStream.close();

        // Entity table
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityTableFile()));
        outputStream.writeObject(this.entityTable.getIndex());
        outputStream.flush();
        outputStream.close();

        // Entity to tables inverted index
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEntityToTablesFile()));
        outputStream.writeObject(this.entityTableLink.getIndex());
        outputStream.flush();
        outputStream.close();

        // Embeddings index
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getEmbeddingsIndexFile()));
        outputStream.writeObject(this.embeddingsIdx.getIndex());
        outputStream.flush();
        outputStream.close();

        // HNSW
        HNSW tmpHNSW = (HNSW) this.hnsw.getIndex();
        outputStream = new ObjectOutputStream(new FileOutputStream(this.outputPath + "/" + Configuration.getHNSWParamsFile()));
        outputStream.writeInt(tmpHNSW.getEmbeddingsDimension());
        outputStream.writeLong(tmpHNSW.getCapacity());
        outputStream.writeInt(tmpHNSW.getNeighborhoodSize());
        outputStream.writeUTF(tmpHNSW.getIndexPath());
        outputStream.flush();
        outputStream.close();
        tmpHNSW.save();

        genNeo4jTableMappings();
    }

    private void genNeo4jTableMappings() throws IOException
    {
        FileOutputStream outputStream = new FileOutputStream(this.outputPath + "/" + Configuration.getTableToEntitiesFile());
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        Iterator<Id> entityIter = ((EntityLinking) this.linker.getLinker()).kgUriIds();

        while (entityIter.hasNext())
        {
            Id entityId = entityIter.next();
            List<String> tables = this.entityTableLink.find(entityId);

            for (String table : tables)
            {
                writer.write("<http://thetis.edao.eu/wikitables/" + table +
                        "> <https://schema.org/mentions> <" + this.entityTable.find(entityId).getUri() + "> .\n");
            }
        }

        writer.flush();
        writer.close();
        outputStream = new FileOutputStream(this.outputPath + "/" + Configuration.getTableToTypesFile());
        writer = new OutputStreamWriter(outputStream);
        Set<String> tables = new HashSet<>();
        Iterator<Id> entityIdIter = ((EntityLinking) this.linker.getLinker()).kgUriIds();

        while (entityIdIter.hasNext())
        {
            List<String> entityTables = this.entityTableLink.find(entityIdIter.next());

            for (String t : entityTables)
            {
                if (tables.contains(t))
                {
                    continue;
                }

                tables.add(t);
                writer.write("<http://thetis.edao.eu/wikitables/" + t +
                        "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                        "<https://schema.org/Table> .\n");
            }
        }

        writer.flush();
        writer.close();
    }

    /**
     * Elapsed time of loading
     * @return Elapsed time of loading
     */
    public long elapsedTime()
    {
        return this.elapsed;
    }

    /**
     * Number of successfully loaded tables
     * @return Number of successfully loaded tables
     */
    public int loadedTables()
    {
        return (int) this.loadedTables.get();
    }

    public int cellsWithLinks()
    {
        return this.cellsWithLinks.get();
    }

    /**
     * Entity linker getter
     * @return Entity linker from link to entity URI
     */
    public EntityLinking getEntityLinker()
    {
        return (EntityLinking) this.linker.getLinker();
    }

    /**
     * Getter to Entity table
     * @return Loaded entity table
     */
    public EntityTable getEntityTable()
    {
        return (EntityTable) this.entityTable.getIndex();
    }

    /**
     * Getter to embeddings index
     * @return Loaded embeddings index
     */
    public EmbeddingsIndex<Id> getEmbeddingsIndex()
    {
        return (EmbeddingsIndex<Id>) this.embeddingsIdx.getIndex();
    }

    /**
     * Getter to HNSW index
     * @return HNSW index
     */
    public HNSW getHNSW()
    {
        return (HNSW) this.hnsw.getIndex();
    }

    /**
     * Getter to entity-table linker
     * @return Loaded entity-table linker
     */
    public EntityTableLink getEntityTableLinker()
    {
        return (EntityTableLink) this.entityTableLink.getIndex();
    }

    public long getApproximateEntityMentions()
    {
        return this.filter.approximateElementCount();
    }
}
