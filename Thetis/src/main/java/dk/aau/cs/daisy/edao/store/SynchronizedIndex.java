package dk.aau.cs.daisy.edao.store;

public class SynchronizedIndex<K, V> implements Index<K, V>
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

    public Index<K, V> getIndex()
    {
        return this.index;
    }
}
