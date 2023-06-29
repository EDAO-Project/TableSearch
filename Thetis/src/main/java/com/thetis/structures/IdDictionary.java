package com.thetis.structures;

import java.io.Serializable;
import java.util.*;

/**
 * Bi-directional mapping dictionary between ID and objects of a specified type
 * @param <K> Object type to be contained
 */
public class IdDictionary<K> extends Dictionary<K, Id> implements Serializable
{
    private Map<K, Id> map;
    private Map<Id, K> inverse;

    public IdDictionary(boolean ordered)
    {
        this(ordered, 10000);
    }

    public IdDictionary(boolean ordered, int initialCapacity)
    {
        if (ordered)
        {
            this.map = new TreeMap<>();
            this.inverse = new TreeMap<>();
        }

        else
        {
            this.map = new HashMap<>(initialCapacity);
            this.inverse = new HashMap<>(initialCapacity);
        }
    }

    @Override
    public int size()
    {
        return this.map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return this.map.isEmpty();
    }

    @Override
    public Enumeration<K> keys()
    {
        return Collections.enumeration(this.map.keySet());
    }

    @Override
    public Enumeration<Id> elements()
    {
        return Collections.enumeration(this.map.values());
    }

    @Override
    public Id get(Object key)
    {
        return this.map.get(key);
    }

    public K get(Id value)
    {
        return this.inverse.get(value);
    }

    @Override
    public Id put(K key, Id id)
    {
        this.inverse.put(id, key);
        return this.map.put(key, id);
    }

    @Override
    public Id remove(Object key)
    {
        Id removed = this.map.remove(key);
        this.inverse.remove(removed);
        return removed;
    }

    public void clear()
    {
        this.map.clear();
        this.inverse.clear();
    }
}
