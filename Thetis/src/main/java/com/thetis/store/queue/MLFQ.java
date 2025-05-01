package com.thetis.store.queue;

import java.util.*;

public class MLFQ<T> implements Queue<T>
{
    private final List<Queue<T>> queues;
    private final int levels;

    public enum Policy
    {
        FIFO, LIFO, CUSTOM
    }

    public MLFQ(int levels, Policy policy)
    {
        this(levels, policy, Comparator.comparingInt(Object::hashCode));
    }

    public MLFQ(int levels, Policy policy, Comparator<T> comparator)
    {
        this.levels = levels;
        this.queues = new ArrayList<>(levels);

        for (int i = 0; i < levels; i++)
        {
            Queue<T> q = switch (policy)
            {
                case FIFO -> new LinkedList<>();
                case LIFO -> Collections.asLifoQueue(new ArrayDeque<>());
                case CUSTOM -> new PriorityQueue<>(comparator);
            };
            this.queues.add(q);
        }
    }

    public int getLevels()
    {
        return this.levels;
    }

    public boolean move(T o, int level)
    {
        if (!remove(o))
        {
            return false;
        }

        return add(o, level);
    }

    public int levelOf(T t)
    {
        for (int i = 0; i < this.levels; i ++)
        {
            if (this.queues.get(i).contains(t))
            {
                return i;
            }
        }

        return -1;
    }

    @Override
    public int size()
    {
        return this.queues.stream().mapToInt(Queue::size).sum();
    }

    @Override
    public boolean isEmpty()
    {
        return this.queues.stream().allMatch(Queue::isEmpty);
    }

    @Override
    public boolean contains(Object o)
    {
        if (!(o instanceof Queue))
        {
            return false;
        }

        T other = (T) o;
        return this.queues.stream().anyMatch(q -> q.contains(other));
    }

    private Collection<T> toCollection()
    {
        List<T> lst = new ArrayList<>();
        this.queues.forEach(lst::addAll);
        return lst;
    }

    @Override
    public Iterator<T> iterator()
    {
        return toCollection().iterator();
    }

    @Override
    public Object[] toArray()
    {
        return toCollection().toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a)
    {
        return toCollection().toArray(a);
    }

    @Override
    public boolean add(T t)
    {
        this.queues.get(0).add(t);
        return true;
    }

    public boolean add(T t, int level)
    {
        if (level < 0 || level >= this.levels)
        {
            return false;
        }

        return this.queues.get(level).add(t);
    }

    @Override
    public boolean remove(Object o)
    {
        for (Queue<T> q : this.queues)
        {
            if (q.remove(o))
            {
                return true;
            }
        }

        return false;
    }

    public boolean remove(Object o, int level)
    {
        if (level < 0 || level > this.levels)
        {
            return false;
        }

        return this.queues.get(level).remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        return c.stream().allMatch(this::contains);
    }

    @Override
    public boolean addAll(Collection<? extends T> c)
    {
        return c.stream().allMatch(this::add);
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        return c.stream().allMatch(this::remove);
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        return this.queues.stream().anyMatch(q -> q.retainAll(c));
    }

    @Override
    public void clear()
    {
        this.queues.forEach(Queue::clear);
    }

    @Override
    public boolean offer(T t)
    {
        return add(t);
    }

    @Override
    public T remove()
    {
        T element = poll();

        if (element == null)
        {
            throw new NoSuchElementException("Queue is empty");
        }

        return element;
    }

    @Override
    public T poll()
    {
        for (int i = 0; i < this.levels; i++)
        {
            if (!this.queues.get(i).isEmpty())
            {
                return this.queues.get(i).remove();
            }
        }

        return null;
    }

    @Override
    public T element()
    {
        T element = peek();

        if (element == null)
        {
            throw new NoSuchElementException("Queue is empty");
        }

        return element;
    }

    @Override
    public T peek()
    {
        for (int i = 0; i < this.levels; i++)
        {
            if (!this.queues.get(i).isEmpty())
            {
                return this.queues.get(i).peek();
            }
        }

        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < this.levels; i++)
        {
            builder.append(i).append(". ").append(this.queues.get(i).toString()).append(" ");
        }

        return builder.toString();
    }
}
