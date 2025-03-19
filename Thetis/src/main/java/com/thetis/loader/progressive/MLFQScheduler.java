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
        if (this.indexables.containsKey(indexable.getId()))
        {
            int oldLevel = (int) this.indexables.get(indexable.getId()).getPriority();
            indexable.setPriority(oldLevel);
            indexable.setPriority(oldLevel);
            this.mlfq.add(indexable, oldLevel);
        }

        else
        {
            this.mlfq.add(indexable);

            int assignedPriority = this.mlfq.levelOf(indexable);
            indexable.setPriority(assignedPriority);
            indexable.setPriority(assignedPriority);
        }

        this.indexables.put(indexable.getId(), indexable);
    }

    @Override
    public synchronized Indexable popIndexable()
    {
        return this.mlfq.poll();
    }

    /*
        The increment is inverted, as level 0 is the highest priority
        An increment of +1 should move the indexable closer towards level 0
     */
    @Override
    public synchronized void update(String id, int levelIncrement)
    {
        if (!this.indexables.containsKey(id))
        {
            return;
        }

        Indexable indexable = this.indexables.get(id);
        int currentLevel = this.mlfq.levelOf(indexable), newLevel;

        if (currentLevel == -1)
        {
            return;
        }

        else if (levelIncrement < 0)
        {
            newLevel = Integer.min(currentLevel + -1 * levelIncrement, this.mlfq.getLevels());
        }

        else
        {
            newLevel = Integer.max(currentLevel - levelIncrement, 0);
        }

        if (!this.mlfq.move(indexable, newLevel))
        {
            throw new IllegalStateException("Failed moving indexable to another MLFQ level");
        }

        indexable.setPriority(newLevel);
        this.indexables.put(indexable.getId(), indexable);
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return this.mlfq.isEmpty();
    }
}
