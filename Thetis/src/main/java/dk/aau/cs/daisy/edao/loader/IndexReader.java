package dk.aau.cs.daisy.edao.loader;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.system.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IndexReader implements IndexIO
{
    private boolean multithreaded;
    private File indexDir;

    // Indexes
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private static final int INDEX_COUNT = 3;

    public IndexReader(File indexDir, boolean isMultithreaded)
    {
        if (!indexDir.isDirectory())
            throw new IllegalArgumentException("'" + indexDir + "' is not a directory");

        else if (!indexDir.exists())
            throw new IllegalArgumentException("'" + indexDir + "' does not exist");

        this.indexDir = indexDir;
        this.multithreaded = isMultithreaded;
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

        while (!f1.isDone() || !f2.isDone() || !f3.isDone());
    }

    private void loadEntityLinker()
    {
        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(this.indexDir + "/" + Configuration.getEntityLinkerFile())))
        {
            this.linker = (EntityLinking) stream.readObject();
        }

        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void loadEntityTable()
    {
        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(this.indexDir + "/" + Configuration.getEntityTableFile())))
        {
            this.entityTable = (EntityTable) stream.readObject();
        }

        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void loadEntityTableLink()
    {
        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(this.indexDir + "/" + Configuration.getEntityToTablesFile())))
        {
            this.entityTableLink = (EntityTableLink) stream.readObject();
        }

        catch (IOException | ClassNotFoundException e)
        {
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
}
