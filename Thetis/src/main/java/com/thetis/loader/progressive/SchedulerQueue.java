package com.thetis.loader.progressive;

import java.util.Collection;

public interface SchedulerQueue
{
    void addIndexable(Indexable indexable);
    void addIndexable(Indexable indexable, int levelIncrement);
    Indexable popIndexable();
    void update(String id, int levelIncrement);
    boolean isEmpty();
    int getLevels();

    default void addIndexables(Collection<Indexable> indexables)
    {
        indexables.forEach(this::addIndexable);
    }
}
