package com.thetis.loader;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.store.lsh.VectorLSHIndex;
import com.thetis.store.lsh.SetLSHIndex;
import com.thetis.structures.Id;
import com.thetis.system.Configuration;
import com.thetis.system.Logger;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Main class responsible for reading indexes serialized on disk
 */
public class IndexReader implements IndexIO
{
    private boolean multithreaded, logProgress;
    private File indexDir;

    // Indexes
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private EmbeddingsIndex<Id> embeddingsIdx;
    private SetLSHIndex typesLSHIndex, predicatesLSHIndex;
    private VectorLSHIndex embeddingsLSHIndex;
    private static final int INDEX_COUNT = 5;

    public IndexReader(File indexDir, boolean isMultithreaded, boolean logProgress)
    {
        if (!indexDir.isDirectory())
        {
            throw new IllegalArgumentException("'" + indexDir + "' is not a directory");
        }

        else if (!indexDir.exists())
        {
            throw new IllegalArgumentException("'" + indexDir + "' does not exist");
        }

        this.indexDir = indexDir;
        this.multithreaded = isMultithreaded;
        this.logProgress = logProgress;
    }

    /**
     * Reads indexes from disk
     * @throws IOException
     */
    @Override
    public void performIO() throws IOException
    {
        ExecutorService threadPoolService = Executors.newFixedThreadPool(this.multithreaded ? INDEX_COUNT : 1);
        Future<?> f1 = threadPoolService.submit(this::loadEntityLinker);
        Future<?> f2 = threadPoolService.submit(this::loadEntityTable);
        Future<?> f3 = threadPoolService.submit(this::loadEntityTableLink);
        Future<?> f4 = threadPoolService.submit(this::loadLSHIndexes);
        Future<?> f5 = threadPoolService.submit(this::loadEmbeddingsIndex);
        int completed = -1;

        while (!f1.isDone() || !f2.isDone() || !f3.isDone() || !f4.isDone() || !f5.isDone())
        {
            int tmpCompleted = (f1.isDone() ? 1 : 0) + (f2.isDone() ? 1 : 0) + (f3.isDone() ? 1 : 0) +
                    (f4.isDone() ? 1 : 0) + (f5.isDone() ? 1 : 0);

            if (tmpCompleted != completed)
            {
                completed = tmpCompleted;
                Logger.log(Logger.Level.INFO, "Loaded indexes: " + completed + "/" + INDEX_COUNT);
            }
        }

        Logger.log(Logger.Level.INFO, "Loaded indexes: " + INDEX_COUNT + "/" + INDEX_COUNT);

        try
        {
            f1.get();
            f2.get();
            f3.get();
            f4.get();
            f5.get();
        }

        catch (InterruptedException | ExecutionException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Failed loading an index:");
            Logger.logNewLine(Logger.Level.ERROR, e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    private void loadEntityLinker()
    {
        this.linker = (EntityLinking) readIndex(this.indexDir + "/" + Configuration.getEntityLinkerFile());
    }

    private void loadEntityTable()
    {
        this.entityTable = (EntityTable) readIndex(this.indexDir + "/" + Configuration.getEntityTableFile());
    }

    private void loadEntityTableLink()
    {
        this.entityTableLink = (EntityTableLink) readIndex(this.indexDir + "/" + Configuration.getEntityToTablesFile());
    }

    private void loadEmbeddingsIndex()
    {
        this.embeddingsIdx = (EmbeddingsIndex<Id>) readIndex(this.indexDir + "/" + Configuration.getEmbeddingsIndexFile());
    }

    private void loadLSHIndexes()
    {
        this.typesLSHIndex = (SetLSHIndex) readIndex(this.indexDir + "/" + Configuration.getTypesLSHIndexFile());
        this.predicatesLSHIndex = (SetLSHIndex) readIndex(this.indexDir + "/" + Configuration.getPredicatesLSHIndexFile());
        this.embeddingsLSHIndex = (VectorLSHIndex) readIndex(this.indexDir + "/" + Configuration.getEmbeddingsLSHFile());
    }

    private Object readIndex(String file)
    {
        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file)))
        {
            return stream.readObject();
        }

        catch (OptionalDataException e)
        {
            if (e.eof)
            {
                Logger.logNewLine(Logger.Level.ERROR, "EOF reached earlier than expected when reading index file: " + file);
            }

            else
            {
                Logger.logNewLine(Logger.Level.ERROR, "Index file stream contains primitive data: " + file);
            }

            throw new RuntimeException(e.getMessage());
        }

        catch (IOException | ClassNotFoundException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "IO error when reading index");
            throw new RuntimeException(e.getMessage());
        }
    }

    public EntityLinking getLinker()
    {
        return this.linker;
    }

    public EntityTable getEntityTable()
    {
        return this.entityTable;
    }

    public EntityTableLink getEntityTableLink()
    {
        return this.entityTableLink;
    }

    public EmbeddingsIndex<Id> getEmbeddingsIndex()
    {
        return this.embeddingsIdx;
    }

    public SetLSHIndex getTypesLSHIndex()
    {
        return this.typesLSHIndex;
    }

    public SetLSHIndex getPredicatesLSHIndex()
    {
        return this.predicatesLSHIndex;
    }

    public VectorLSHIndex getEmbeddingsLSHIndex()
    {
        return this.embeddingsLSHIndex;
    }
}
