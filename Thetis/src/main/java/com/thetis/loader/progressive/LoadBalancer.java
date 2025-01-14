package com.thetis.loader.progressive;

import java.util.Queue;

public interface LoadBalancer
{
    void add(Indexable indexable);
    int queues();
    Queue<Indexable> getQueue(int index);
}
