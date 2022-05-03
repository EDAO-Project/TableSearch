package dk.aau.cs.daisy.edao.store;

import java.io.Serializable;

public class SynchronizedIndex<K, V> implements Index<K, V>, Serializable
{
    private Index<K, V> index;

    public static <Key, Value> SynchronizedIndex<Key, Value> wrap(Index<Key, Value> index)
    {
        return new SynchronizedIndex<>(index);
    }

    public SynchronizedIndex(Index<K, V> index)
    {
        this.index = index;
    }

    @Override
    public synchronized void insert(K key, V value)
    {
        this.index.insert(key, value);
    }

    @Override
    public synchronized boolean remove(K key)
    {
        return this.index.remove(key);
    }

    @Override
    public synchronized V find(K key)
    {
        return this.index.find(key);
    }

    @Override
    public synchronized boolean contains(K key)
    {
        return this.index.contains(key);
    }

    @Override
    public int size()
    {
        return this.index.size();
    }

    @Override
    public void clear()
    {
        this.index.clear();
    }

    public Index<K, V> getIndex()
    {
        return this.index;
    }
}
