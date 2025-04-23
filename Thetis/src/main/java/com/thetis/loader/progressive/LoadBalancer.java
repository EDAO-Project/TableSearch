package com.thetis.loader.progressive;

import java.util.Queue;

public interface LoadBalancer
{
    void add(Indexable indexable);
    void add(Indexable indexable, int index);
    int queues();
    Queue<Indexable> getQueue(int index);
}
