package com.thetis.loader.progressive;

import java.util.*;

public class PrioritySchedulerQueue implements SchedulerQueue
{
    private final TreeMap<Double, Set<Indexable>> map = new TreeMap<>((e1, e2) -> Double.compare(e2, e1));
    private final Map<String, Double> invIndex = new HashMap<>();

    /**
     * Add an indexable to the priority queue
     * @param indexable Indexable to add to the priority queue
     */
    @Override
    public synchronized void addIndexable(Indexable indexable)
    {
        addIndexable(indexable, 0);
    }

    @Override
    public void addIndexable(Indexable indexable, int levelIncrement)
    {
        if (!this.map.containsKey(indexable.getPriority()))
        {
            indexable.setPriority(indexable.getPriority() + levelIncrement);
            this.map.put(indexable.getPriority() + levelIncrement, new HashSet<>());
        }

        this.map.get(indexable.getPriority() + levelIncrement).add(indexable);
        this.invIndex.put(indexable.getId(), indexable.getPriority() + levelIncrement);
        indexable.setPriority(indexable.getPriority() + levelIncrement);
    }

    /**
     * Retrieves, removes, and returns one of the indexables belonging to the set of indexables of the highest priority
     * Order of retrieval and removal from this set of indexables is determined by the HashSet
     * @return Indexable of the highest priority
     */
    @Override
    public synchronized Indexable popIndexable()
    {
        Indexable popped = this.map.firstEntry().getValue().iterator().next();
        remove(popped);

        return popped;
    }

    /**
     * Updates the indexable of the given ID according to the caller
     * @param id ID of indexable to update
     * @param levelIncrement How much to update its priority by
     */
    @Override
    public synchronized void update(String id, int levelIncrement)
    {
        if (this.invIndex.containsKey(id))
        {
            double priority = this.invIndex.get(id);
            Set<Indexable> indexables = this.map.get(priority);
            Indexable[] indexableArray = indexables.toArray(new Indexable[0]);

            for (int i = 0; i < indexableArray.length; i++)
            {
                if (indexableArray[i].getId().equals(id))
                {
                    remove(indexableArray[i]);
                    indexableArray[i].setPriority(indexableArray[i].getPriority() + levelIncrement);
                    addIndexable(indexableArray[i]);
                    break;
                }
            }
        }
    }

    @Override
    public boolean isEmpty()
    {
        return this.map.isEmpty();
    }

    private void remove(Indexable indexable)
    {
        if (this.map.containsKey(indexable.getPriority()))
        {
            this.map.get(indexable.getPriority()).remove(indexable);

            if (this.map.get(indexable.getPriority()).isEmpty())
            {
                this.map.remove(indexable.getPriority());
            }
        }

        this.invIndex.remove(indexable.getId());
    }

    @Override
    public int getLevels()
    {
        return this.map.size();
    }

    public int countPriorities()
    {
        return this.map.size();
    }

    public int countElements()
    {
        return this.invIndex.size();
    }

    public synchronized Map<String, Double> getPriorities()
    {
        return new HashMap<>(this.invIndex);
    }
}
