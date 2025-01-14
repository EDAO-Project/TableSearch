package com.thetis.loader.progressive;

import java.util.Collection;

public interface SchedulerQueue
{
    void addIndexable(Indexable indexable);
    Indexable popIndexable();
    void update(String id, int levelIncrement);
    boolean isEmpty();

    default void addIndexables(Collection<Indexable> indexables)
    {
        indexables.forEach(this::addIndexable);
    }
}
