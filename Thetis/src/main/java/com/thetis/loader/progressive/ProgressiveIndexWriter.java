package com.thetis.loader.progressive;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.IndexWriter;
import com.thetis.loader.Linker;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.Pair;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.system.Logger;
import com.thetis.tables.JsonTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Main class for progressively indexing of tables in an adaptive fashion
 * It guarantees that the indexes will converge, but it will also adapt its indexing of table rows
 */
public class ProgressiveIndexWriter extends IndexWriter implements ProgressiveIndexIO
{
    private final Pool indexers;
    private final Runnable cleanupProcess;
    private Thread schedulerThread;
    private Thread loadIndexables;
    private boolean isRunning = false, isPaused = false;
    private final PriorityScheduler scheduler;
    private final int corpusSize;
    private final HashSet<String> insertedIds = new HashSet<>();
    private final Map<String, Integer> tableSizes = new HashMap<>();
    private int  totalRows = 0;
    private final AtomicInteger indexedRows = new AtomicInteger(0);
    private boolean totalTableRowsInitialized = false;
    private long prevTimePoint;

    public ProgressiveIndexWriter(List<Path> files, File indexPath, Linker entityLinker,
                                  Neo4jEndpoint neo4j, int threads, DBDriverBatch<List<Double>, String> embeddingStore,
                                  String wikiPrefix, String uriPrefix, PriorityScheduler scheduler, Runnable cleanup)
    {
        super(files, indexPath, entityLinker, neo4j, threads, embeddingStore, wikiPrefix, uriPrefix);
        this.scheduler = scheduler;
        this.cleanupProcess = cleanup;
        this.corpusSize = files.size();
        this.indexers = new PullIndexingPool(this::indexTable, this::supplyIndexable, threads);
        this.loadIndexables = new Thread(() -> {
            for (Path path : files)
            {
                IndexTable it = new IndexTable(path, this::indexRow, true);
                this.scheduler.addIndexTable(it);
            }
        });
        this.loadIndexables.start();
    }

    public void setTotalRows(int rows)
    {
        this.totalRows = rows;
        this.totalTableRowsInitialized = true;
    }

    /**
     * Starts thread to progressively index tables
     */
    @Override
    public void performIO()
    {
        try
        {
            Thread.sleep(2500);
        }

        catch (InterruptedException ignored) {}

        Runnable indexing = () -> {
            this.prevTimePoint = System.currentTimeMillis();

            while (!this.indexers.isCompleted())
            {
                try
                {
                    Thread.sleep(30000);
                }

                catch (InterruptedException ignored) {}
            }

            this.cleanupProcess.run();
            finalizeIndexing();
            this.isRunning = false;
        };
        this.schedulerThread = new Thread(indexing);
        this.schedulerThread.start();
        this.isRunning = true;
    }

    private void finalizeIndexing()
    {
        try
        {
            this.indexers.stopIndexing();
            Logger.log(Logger.Level.INFO, "Collecting IDF weights...");
            loadIDFs();

            Logger.log(Logger.Level.INFO, "Writing indexes on disk...");
            flushToDisk();
            Logger.log(Logger.Level.INFO, "Progressive indexing has completed");
        }

        catch (IOException e)
        {
            throw new RuntimeException("Exception during progressive indexing: " + e.getMessage());
        }
    }

    private void indexTable(Indexable indexable)
    {
        if (indexable.index() != null)
        {
            synchronized (super.lock)
            {
                int tableSize = indexable.getIndexable().rows.size();
                boolean isFirstTime = false;

                if (!this.tableSizes.containsKey(indexable.getId()))
                {
                    this.tableSizes.put(indexable.getId(), tableSize);
                    this.totalRows += this.totalTableRowsInitialized ? 0 : tableSize;
                    isFirstTime = true;
                }

                String indexedPercentage = String.valueOf(((double) this.indexedRows.get() / this.totalRows) * 100);

                if (System.currentTimeMillis() - this.prevTimePoint > 1000) // Once a second
                {
                    this.prevTimePoint = System.currentTimeMillis();
                    Logger.log(Logger.Level.INFO, "Indexed " + (indexedPercentage.contains("E") ? "0.00" : indexedPercentage) + "%");
                }

                if (!indexable.isIndexed()) // Because the entire indexable is popped from the scheduler when selected and then also from the indexing pool
                {
                    if (isFirstTime)
                    {
                        this.scheduler.addIndexTable(indexable, this.scheduler.priorities() - 1);
                    }

                    else
                    {
                        this.scheduler.addIndexTable(indexable, (int) indexable.getPriority() + 1);
                    }
                }

                else
                {
                    Logger.log(Logger.Level.INFO, "Fully indexed " + super.loadedTables.incrementAndGet() + "/" + this.corpusSize + " tables");
                }

                this.insertedIds.add(indexable.getId());
            }
        }
    }

    private void indexRow(String id, int row, List<JsonTable.TableCell> rowToindex)
    {
        int entities = rowToindex.size(), column = 0;
        List<String> indexedRow = new ArrayList<>();

        for (int i = 0; i < entities; i++)
        {
            if (!rowToindex.get(i).links.isEmpty())
            {
                super.cellsWithLinks.incrementAndGet();
                String tableEntity = rowToindex.get(i).links.get(0), linkedEntity = null;
                List<String> matchesUris = new ArrayList<>();

                if ((linkedEntity = super.linker.mapTo(tableEntity)) != null)
                {
                    matchesUris.add(linkedEntity);
                }

                else
                {
                    linkedEntity = super.entityLinker.link(tableEntity.replace("http://www.", "http://en."));

                    if (linkedEntity != null)
                    {
                        List<String> entityTypes = super.neo4j.searchTypes(linkedEntity);
                        List<String> entityPredicates = super.neo4j.searchPredicates(linkedEntity);
                        matchesUris.add(linkedEntity);
                        super.linker.addMapping(tableEntity, linkedEntity);

                        for (String type : super.disallowedEntityTypes)
                        {
                            entityTypes.remove(type);
                        }

                        Id entityId = ((EntityLinking) super.linker.getLinker()).kgUriLookup(linkedEntity);
                        List<Double> embeddings = super.embeddingsDB.select(linkedEntity.replace("'", "''"));
                        this.hnsw.insert(linkedEntity, Collections.emptySet());
                        super.entityTable.insert(entityId,
                                new Entity(linkedEntity, entityTypes.stream().map(Type::new).collect(Collectors.toList()), entityPredicates));

                        if (embeddings != null)
                        {
                            super.embeddingsIdx.insert(entityId, embeddings);
                        }
                    }
                }

                if (super.linker.mapTo(tableEntity) != null)
                {
                    String entity = super.linker.mapTo(tableEntity);
                    Id entityId = ((EntityLinking) super.linker.getLinker()).kgUriLookup(entity);
                    Pair<Integer, Integer> location = new Pair<>(row, column);

                    synchronized (super.lock)
                    {
                        ((EntityTableLink) super.entityTableLink.getIndex()).
                                addLocation(entityId, id, List.of(location));
                    }
                }

                if (!matchesUris.isEmpty())
                {
                    for (String entity : matchesUris)
                    {
                        synchronized (this.lock)
                        {
                            super.filter.put(entity);
                            indexedRow.add(entity);
                        }
                    }
                }
            }

            column++;
        }

        this.indexedRows.incrementAndGet();
    }

    private Indexable supplyIndexable()
    {
        if (this.scheduler.hasNext())
        {
            Indexable item = this.scheduler.next();

            if (item != null)
            {
                Logger.logNewLine(Logger.Level.DEBUG, "Indexing " + item.getId() + " (" + item.getPriority() + ")");
                return item;
            }
        }

        return null;
    }

    /**
     * Adds a new table to the scheduler and assigns the table the currently highest priority
     * @param tablePath Path to the table file
     * @return True if the table was added to the scheduler and false if the table file could not be parsed
     */
    @Override
    public boolean addTable(Path tablePath)
    {
        IndexTable tableToIndex = new IndexTable(tablePath, this::indexRow, true);
        this.scheduler.addIndexTable(tableToIndex);

        return true;
    }

    /**
     * Waits until the scheduler has completed all of its tasks
     */
    @Override
    public void waitForCompletion()
    {
        try
        {
            this.schedulerThread.join();
            this.isRunning = false;
        }

        catch (InterruptedException ignored) {}
    }

    /**
     * Pauses indexing
     */
    @Override
    public synchronized void pauseIndexing()
    {
        this.isPaused = true;
        this.isRunning = false;
        this.indexers.pause();
    }

    /**
     * Continues indexing after being paused
     */
    @Override
    public synchronized void continueIndexing()
    {
        this.isPaused = false;
        this.isRunning = true;
        this.schedulerThread.interrupt();
        this.indexers.resume();
    }

    /**
     * Checks whether the progressive indexing is still running
     * @return True if progressive indexing is currently running
     */
    public boolean isRunning()
    {
        return this.isRunning;
    }

    /**
     * Allows updating indexables externally
     * @param id ID of indexable to update
     * @param increment How much an indexable should be incremented in priority
     */
    public void updateIndexable(String id, int increment)
    {
        this.scheduler.update(id, increment);
    }

    public double indexed()
    {
        synchronized (super.lock)
        {
            if (this.totalTableRowsInitialized)
            {
                return (double) this.indexedRows.get() / this.totalRows;
            }

            return (double) this.indexedRows.get() / this.tableSizes.values().stream().mapToInt(i -> i).sum();
        }
    }

    public synchronized PriorityScheduler getScheduler()
    {
        return this.scheduler;
    }
}
