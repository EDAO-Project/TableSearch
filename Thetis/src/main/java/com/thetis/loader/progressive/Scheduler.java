package com.thetis.loader.progressive;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

public interface Scheduler extends Iterator<Indexable>
{
    void addIndexTables(Collection<Indexable> indexTables);
    void addIndexTable(Indexable indexTable);
    void update(String id, Consumer<Indexable> update);
}
