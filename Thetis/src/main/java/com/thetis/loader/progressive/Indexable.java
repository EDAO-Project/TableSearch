package com.thetis.loader.progressive;

import com.thetis.tables.JsonTable;

import java.nio.file.Path;

public interface Indexable
{
    Object index();
    JsonTable getIndexable();
    Path getPath();
    String getId();
    double getPriority();
    void setPriority(double priority);
    boolean isIndexed();
}
