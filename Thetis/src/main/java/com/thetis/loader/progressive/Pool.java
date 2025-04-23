package com.thetis.loader.progressive;

public interface Pool
{
    void queue(Indexable indexable);
    void stopIndexing();
    boolean isCompleted();
}
