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
import com.thetis.structures.table.DynamicTable;
import com.thetis.structures.table.Table;
import com.thetis.system.Logger;
import com.thetis.tables.JsonTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Main class for progressively indexing of tables in an adaptive fashion
 * It guarantees that the indexes will converge, but it will also adapt its indexing of table rows
 */
public class ProgressiveIndexWriter extends IndexWriter implements ProgressiveIndexIO
{
    private final Runnable cleanupProcess;
    private Thread schedulerThread;
    private boolean isRunning = false, isPaused = false;
    private final Scheduler scheduler;
    private final Map<String, Table<String>> indexedTables = new HashMap<>();
    private final int corpusSize;
    private int largestTable = 0;
    private double maxPriority = -1.0;
    private final HashSet<String> insertedIds = new HashSet<>();

    public ProgressiveIndexWriter(List<Path> files, File indexPath, Linker entityLinker,
                                  Neo4jEndpoint neo4j, int threads, DBDriverBatch<List<Double>, String> embeddingStore,
                                  String wikiPrefix, String uriPrefix, Scheduler scheduler, Runnable cleanup)
    {
        super(files, indexPath, entityLinker, neo4j, threads, embeddingStore, wikiPrefix, uriPrefix);
        this.scheduler = scheduler;
        this.cleanupProcess = cleanup;
        this.corpusSize = files.size();

        for (Path path : files)
        {
            IndexTable it = new IndexTable(path, this::indexRow);
            this.scheduler.addIndexTable(it);
        }
    }

    /**
     * Starts thread to progressively index tables
     */
    @Override
    public void performIO()
    {
        Runnable indexing = () -> {
            while (this.scheduler.hasNext())
            {
                while (this.isPaused)
                {
                    try
                    {
                        Thread.sleep(10000);
                    }

                    catch (InterruptedException ignore) {}
                }

                Indexable item = this.scheduler.next();

                if (item.index() != null)
                {
                    int tableSize = item.getIndexable().rows.size();
                    double decrement = (double) this.largestTable / tableSize;
                    item.setPriority(item.getPriority() - decrement);
                    this.largestTable = Math.max(this.largestTable, tableSize);
                    this.maxPriority = Math.max(this.maxPriority, item.getPriority() - decrement);

                    synchronized (super.lock)
                    {
                        if (!item.isIndexed())
                        {
                            this.scheduler.addIndexTable(item);
                        }

                        else
                        {
                            Logger.log(Logger.Level.INFO, "Fully indexed " + super.loadedTables.incrementAndGet() + "/" + this.corpusSize + " tables");
                        }

                        if (!this.insertedIds.contains(item.getId()))
                        {
                            this.insertedIds.add(item.getId());
                        }
                    }
                }
            }

            this.cleanupProcess.run();
            this.isRunning = false;
            finalizeIndexing();
        };
        this.schedulerThread = new Thread(indexing);
        this.schedulerThread.start();
        this.isRunning = true;
    }

    private void finalizeIndexing()
    {
        try
        {
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
                        synchronized (super.lock)
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

        if (!this.indexedTables.containsKey(id))
        {
            this.indexedTables.put(id, new DynamicTable<>());
        }

        this.indexedTables.get(id).addRow(new Table.Row<>(indexedRow));
    }

    /**
     * Adds a new table to the scheduler and assigns the table the currently highest priority
     * @param tablePath Path to the table file
     * @return True if the table was added to the scheduler and false if the table file could not be parsed
     */
    @Override
    public boolean addTable(Path tablePath)
    {
        IndexTable tableToIndex = new IndexTable(tablePath, this.maxPriority, this::indexRow);
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
     * @param update Procedure for updating the identified indexable
     */
    public void updatePriority(String id, Consumer<Indexable> update)
    {
        this.scheduler.update(id, update);
    }

    public int getLargestTable()
    {
        return this.largestTable;
    }

    public double getMaxPriority()
    {
        return this.maxPriority;
    }
}
