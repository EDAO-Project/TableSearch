package dk.aau.cs.daisy.edao.store.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class BucketIndex<K, V> implements Serializable
{
    private List<Bucket<K, V>> buckets;

    protected BucketIndex(int buckets)
    {
        this.buckets = new ArrayList<>();

        for (int i = 0; i < buckets; i++)
        {
            this.buckets.add(new Bucket<>());
        }
    }

    public int buckets()
    {
        return this.buckets.size();
    }

    protected void add(int idx, K key, V value)
    {
        this.buckets.get(idx).add(key, value);
    }

    protected Set<V> get(int idx)
    {
        return this.buckets.get(idx).all();
    }

    @Override
    public String toString()
    {
        return this.buckets.toString();
    }
}
