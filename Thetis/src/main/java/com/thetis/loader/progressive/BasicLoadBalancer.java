package com.thetis.loader.progressive;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BasicLoadBalancer implements LoadBalancer
{
    private final List<Queue<Indexable>> indexables;

    public BasicLoadBalancer(int pools)
    {
        this.indexables = new ArrayList<>(pools);

        for (int i = 0; i < pools; i++)
        {
            this.indexables.add(new LinkedList<>());
        }
    }

    @Override
    public void add(Indexable indexable)
    {
        int smallest = Integer.MAX_VALUE, smallestIndex = -1;
        int queues = this.indexables.size();

        for (int i = 0; i < queues; i++)
        {
            if (this.indexables.get(i).size() < smallest)
            {
                smallest = this.indexables.get(i).size();
                smallestIndex = i;
            }
        }

        this.indexables.get(smallestIndex).add(indexable);
    }

    @Override
    public int queues()
    {
        return this.indexables.size();
    }

    @Override
    public Queue<Indexable> getQueue(int index)
    {
        if (index < 0 || index >= this.indexables.size())
        {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + this.indexables.size());
        }

        return this.indexables.get(index);
    }
}
