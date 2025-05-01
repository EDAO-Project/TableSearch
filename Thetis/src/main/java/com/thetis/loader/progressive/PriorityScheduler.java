package com.thetis.loader.progressive;

import java.util.Collection;

public class PriorityScheduler implements Scheduler
{
    private final SchedulerQueue queue = new MLFQScheduler();

    @Override
    public void addIndexTable(Indexable indexTable)
    {
        indexTable.setPriority(0);
        addIndexTable(indexTable, 0);
    }

    @Override
    public void addIndexTables(Collection<Indexable> indexTables)
    {
        this.queue.addIndexables(indexTables);
    }

    @Override
    public void addIndexTable(Indexable indexTable, int level)
    {
        this.queue.addIndexable(indexTable, level);
    }

    @Override
    public boolean hasNext()
    {
        return !this.queue.isEmpty();
    }

    @Override
    public Indexable next()
    {
        if (!hasNext())
        {
            return null;
        }

        return this.queue.popIndexable();
    }

    @Override
    public void update(String id, int increment)
    {
        this.queue.update(id, increment);
    }

    @Override
    public String toString()
    {
        return this.queue.toString();
    }

    public int priorities()
    {
        return this.queue.getLevels();
    }
}
