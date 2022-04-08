package dk.aau.cs.daisy.edao.structures;

import java.util.*;

public class IdDictionary<K> extends Dictionary<K, Id>
{
    private Map<K, Id> map;

    public IdDictionary(boolean ordered)
    {
        this(ordered, 10000);
    }

    public IdDictionary(boolean ordered, int initialCapacity)
    {
        if (ordered)
            this.map = new TreeMap<>();

        else
            this.map = new HashMap<>();
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

    @Override
    public Id put(K key, Id id)
    {
        return this.map.put(key, id);
    }

    @Override
    public Id remove(Object key)
    {
        return this.map.remove(key);
    }
}
