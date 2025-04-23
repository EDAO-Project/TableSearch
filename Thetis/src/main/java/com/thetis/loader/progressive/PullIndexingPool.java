package com.thetis.loader.progressive;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PullIndexingPool implements Pool
{
    private final List<Thread> threads;
    private final Consumer<Indexable> indexer;
    private final Supplier<Indexable> supplier;
    private final List<Boolean> completeStatus = new ArrayList<>();

    public PullIndexingPool(Consumer<Indexable> indexer, Supplier<Indexable> supplier, int threads)
    {
        this.threads = new ArrayList<>(threads);
        this.indexer = indexer;
        this.supplier = supplier;

        for (int i = 0; i < threads; i++)
        {
            int pool = i;
            Thread indexingThread = new Thread(() -> indexer(pool));
            indexingThread.start();
            this.threads.add(indexingThread);
            this.completeStatus.add(false);
        }
    }

    @Override
    public void queue(Indexable indexable)
    {

    }

    @Override
    public void stopIndexing()
    {
        for (Thread thread : threads)
        {
            thread.interrupt();
        }
    }

    @Override
    public boolean isCompleted()
    {
        return this.completeStatus.stream().allMatch(status -> status);
    }

    private void indexer(int pool)
    {
        int nullCount = 0;
        boolean seenFirst = false;

        while (true)
        {
            Indexable indexable = this.supplier.get();

            if (indexable != null)
            {
                nullCount = 0;
                seenFirst = true;
                this.indexer.accept(indexable);

                if (this.completeStatus.get(pool))
                {
                    this.completeStatus.set(pool, false);
                }
            }

            else if (++nullCount == 100 && seenFirst)
            {
                this.completeStatus.set(pool, true);
            }
        }
    }
}
