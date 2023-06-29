package com.thetis.store.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Bucket<K, V> implements Serializable
{
    private final List<K> keys;
    private final List<V> values;

    public Bucket()
    {
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
    }

    /**
     * Adds to bucket a mapping from key to value
     */
    public void add(K key, V value)
    {
        this.keys.add(key);
        this.values.add(value);
    }

    /**
     * Finds all mapping of key and returns their value in a set
     * @param key Search key
     * @return all values that are mapped to by the given key
     */
    public Set<V> find(K key)
    {
        int keys = this.keys.size();
        Set<V> values = new HashSet<>();

        for (int i = 0; i < keys; i++)
        {
            if (this.keys.get(i).equals(key))
                values.add(this.values.get(i));
        }

        return values;
    }

    /**
     * @return All values store in bucket
     */
    public Set<V> all()
    {
        return new HashSet<>(this.values);
    }

    /**
     * Deletes all mapping for the given key
     * @param key Search key
     * @return True if the key is found and at least one mapping exists
     */
    public boolean delete(K key)
    {
        boolean found = false;

        for (int i = 0; i < keys.size(); i++)
        {
            if (this.keys.get(i).equals(key))
            {
                this.keys.remove(i);
                this.values.remove(i);
                found = true;
                i--;
            }
        }

        return found;
    }

    @Override
    public String toString()
    {
        return this.keys + " " + this.values;
    }
}
