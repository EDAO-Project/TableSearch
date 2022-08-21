package dk.aau.cs.daisy.edao.loader;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.store.lsh.TypesLSHIndex;
import dk.aau.cs.daisy.edao.store.lsh.VectorLSHIndex;
import dk.aau.cs.daisy.edao.system.Configuration;
import dk.aau.cs.daisy.edao.system.Logger;

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
    private TypesLSHIndex typesLSHIndex;
    private VectorLSHIndex embeddingsLSHIndex;
    private static final int INDEX_COUNT = 4;

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
        int completed = -1;

        while (!f1.isDone() || !f2.isDone() || !f3.isDone() || f4.isDone())
        {
            int tmpCompleted = (f1.isDone() ? 1 : 0) + (f2.isDone() ? 1 : 0) + (f3.isDone() ? 1 : 0) + (f4.isDone() ? 1 : 0);

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

    private void loadLSHIndexes()
    {
        this.typesLSHIndex = (TypesLSHIndex) readIndex(this.indexDir + "/" + Configuration.getTypesLSHIndexFile());
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

    public TypesLSHIndex getTypesLSHIndex()
    {
        return this.typesLSHIndex;
    }

    public VectorLSHIndex getEmbeddingsLSHIndex()
    {
        return this.embeddingsLSHIndex;
    }
}
