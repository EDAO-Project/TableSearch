package com.thetis.loader.progressive;

import java.util.*;
import java.util.function.Consumer;

public class IndexingPool implements Pool
{
    private final List<Thread> threads;
    private final Consumer<Indexable> indexer;
    private final LoadBalancer balancer;
    private boolean isPaused = false;

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

    @Override
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
                while (this.balancer.getQueue(pool).isEmpty() || this.isPaused)
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
        return status().stream().allMatch(s -> s == 0);
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

    @Override
    public void pause()
    {
        this.isPaused = true;
    }

    @Override
    public void resume()
    {
        this.isPaused = false;
    }
}
