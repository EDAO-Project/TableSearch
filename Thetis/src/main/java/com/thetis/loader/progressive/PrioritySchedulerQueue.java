package com.thetis.loader.progressive;

import java.util.*;
import java.util.function.Consumer;

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
        if (!this.map.containsKey(indexable.getPriority()))
        {
            this.map.put(indexable.getPriority(), new HashSet<>());
        }

        this.map.get(indexable.getPriority()).add(indexable);
        this.invIndex.put(indexable.getId(), indexable.getPriority());
    }

    /**
     * Adds a collection of indexables to the priority queue
     * @param indexables Collection of indexables to add to the priority queue
     */
    @Override
    public synchronized void addIndexables(Collection<Indexable> indexables)
    {
        indexables.forEach(this::addIndexable);
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
     * @param id ID of the indexable to update
     * @param update Lambda to update the indexable
     */
    @Override
    public synchronized void update(String id, Consumer<Indexable> update)
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
                    update.accept(indexableArray[i]);
                    addIndexable(indexableArray[i]);
                    break;
                }
            }
        }
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

    public int countPriorities()
    {
        return this.map.size();
    }

    public int countElements()
    {
        return this.invIndex.size();
    }

    public List<Double> getPriorities()
    {
        return new ArrayList<>(this.invIndex.values());
    }
}
