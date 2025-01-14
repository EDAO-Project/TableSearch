package com.thetis.loader.progressive;

import com.thetis.store.queue.MLFQ;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class MLFQScheduler implements SchedulerQueue
{
    private final MLFQ<Indexable> mlfq;
    private final Map<String, Indexable> indexables = new HashMap<>();
    private static final int LEVELS = 5;
    private static final MLFQ.Policy POLICY = MLFQ.Policy.FIFO;
    private static final Comparator<Indexable> SRPT_COMPARATOR =
            Comparator.comparingInt(i -> i.getIndexable().rows.size());

    public MLFQScheduler()
    {
        this.mlfq = new MLFQ<>(LEVELS, POLICY, SRPT_COMPARATOR);
    }

    @Override
    public synchronized void addIndexable(Indexable indexable)
    {
        Indexable copy = new IndexTable(indexable.getPath(), indexable.getPriority(), (id, row, item) -> {}, false);

        if (this.indexables.containsKey(indexable.getId()))
        {
            int oldLevel = (int) this.indexables.get(indexable.getId()).getPriority();
            indexable.setPriority(oldLevel);
            copy.setPriority(oldLevel);
            this.mlfq.add(indexable, oldLevel);
        }

        else
        {
            this.mlfq.add(indexable);

            int assignedPriority = this.mlfq.levelOf(indexable);
            copy.setPriority(assignedPriority);
            indexable.setPriority(assignedPriority);
        }

        this.indexables.put(copy.getId(), copy);
    }

    @Override
    public synchronized Indexable popIndexable()
    {
        return this.mlfq.poll();
    }

    @Override
    public synchronized void update(String id, int levelIncrement)
    {
        if (!this.indexables.containsKey(id))
        {
            return;
        }

        Indexable copy = this.indexables.get(id);
        int currentLevel = this.mlfq.levelOf(copy), newLevel;

        if (currentLevel == -1)
        {
            return;
        }

        else if (levelIncrement < 0)
        {
            newLevel = Integer.max(currentLevel + levelIncrement, 0);
        }

        else
        {
            newLevel = Integer.min(currentLevel + levelIncrement, this.mlfq.getLevels());
        }

        if (!this.mlfq.move(copy, newLevel))
        {
            throw new IllegalStateException("Failed moving indexable to another MLFQ level");
        }

        copy.setPriority(newLevel);
        this.indexables.put(copy.getId(), copy);
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return this.mlfq.isEmpty();
    }
}
