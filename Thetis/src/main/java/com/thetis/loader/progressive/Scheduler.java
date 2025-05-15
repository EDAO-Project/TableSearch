package com.thetis.loader.progressive;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface Scheduler extends Iterator<Indexable>
{
    void addIndexTables(Collection<Indexable> indexTables);
    void addIndexTable(Indexable indexTable);
    void addIndexTable(Indexable indexTable, int increment);
    void update(String id, int increment);
    Map<String, Indexable> getIndexables();
}
