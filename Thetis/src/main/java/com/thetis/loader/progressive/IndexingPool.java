package com.thetis.loader.progressive;

import java.util.*;
import java.util.function.Consumer;

public class IndexingPool
{
    private final List<Thread> threads;
    private final Consumer<Indexable> indexer;
    private final LoadBalancer balancer;

    public IndexingPool(LoadBalancer balancer, Consumer<Indexable> indexer)
    {
        int threads = balancer.queues();
        this.threads = new ArrayList<>(threads);
        this.balancer = balancer;
        this.indexer = indexer;

        for (int i = 0; i < threads; i++)
        {
            int pool = i;
            Thread indexingThread = new Thread(() -> indexer(pool));
            indexingThread.start();
            this.threads.add(indexingThread);
        }
    }

    public void queue(Indexable indexable)
    {
        this.balancer.add(indexable);
    }

    private void indexer(int pool)
    {
        while (true)
        {
            try
            {
                while (this.balancer.getQueue(pool).isEmpty())
                {
                    Thread.sleep(250);
                }

                Indexable indexable = this.balancer.getQueue(pool).poll();

                if (indexable != null)
                {
                    this.indexer.accept(indexable);
                }
            }

            catch (InterruptedException ignored) {}
        }
    }

    public void stopIndexing()
    {
        for (Thread thread : threads)
        {
            thread.interrupt();
        }
    }

    public List<Integer> status()
    {
        int threads = this.threads.size();
        List<Integer> status = new ArrayList<>(threads);

        for (int i = 0; i < threads; i++)
        {
            status.add(this.balancer.getQueue(i).size());
        }

        return status;
    }
}
