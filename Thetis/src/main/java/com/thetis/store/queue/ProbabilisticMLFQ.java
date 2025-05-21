package com.thetis.store.queue;

import java.util.Comparator;
import java.util.Random;

public class ProbabilisticMLFQ<T> extends MLFQ<T>
{
    private static final Random random = new Random();
    private static final int RANGE = 100;

    public ProbabilisticMLFQ(int levels, Policy policy)
    {
        super(levels, policy);
    }

    public ProbabilisticMLFQ(int levels, Policy policy, Comparator<T> comparator)
    {
        super(levels, policy, comparator);
    }

    private int probSelectQueue()
    {
        int prop = random.nextInt() % RANGE, threshold = RANGE / 2;

        for (int index = 0; index < super.levels; index++)
        {
            if (prop >= threshold)
            {
                return index;
            }

            threshold /= 2;
        }

        return super.levels - 1;
    }

    @Override
    public T poll()
    {
        int queueIndex = probSelectQueue();

        while (super.queues.get(queueIndex).isEmpty())
        {
            queueIndex = probSelectQueue();
        }

        return super.queues.get(queueIndex).remove();
    }
}
