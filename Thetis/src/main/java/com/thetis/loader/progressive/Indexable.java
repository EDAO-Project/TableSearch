package com.thetis.loader.progressive;

import com.thetis.tables.JsonTable;

public interface Indexable
{
    Object index();
    JsonTable getIndexable();
    String getId();
    double getPriority();
    void setPriority(double priority);
    boolean isIndexed();
}
