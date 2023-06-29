package com.thetis.store.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BucketGroup<K, V> implements Serializable
{
    private List<Bucket<K, V>> buckets;

    public BucketGroup(int buckets)
    {
        this.buckets = new ArrayList<>(buckets);

        for (int i = 0; i < buckets; i++)
        {
            this.buckets.add(new Bucket<>());
        }
    }

    @Override
    public String toString()
    {
        return "{" + this.buckets.toString() + "}";
    }

    /**
     * Add key-value pair to bucket
     * @param idx Index of bucket to add key-value in
     * @param key Key of value to be added
     * @param value Value to be added
     */
    public void add(int idx, K key, V value)
    {
        this.buckets.get(idx).add(key, value);
    }

    /**
     * Number of buckets in bucket group
     * @return Number of buckets
     */
    public int size()
    {
        return this.buckets.size();
    }

    /**
     * Returns all values in bucket at specified index
     * @param idx Bucket index
     * @return All values in bucket
     */
    public Set<V> get(int idx)
    {
        return this.buckets.get(idx).all();
    }

    /**
     * All buckets in group
     * @return All buckets in group
     */
    public List<Bucket<K, V>> buckets()
    {
        return this.buckets;
    }
}
