package com.thetis.store;

public interface Index<K, V>
{
    void insert(K key, V value);
    boolean remove(K key);
    V find(K key);
    boolean contains(K key);
    int size();
    void clear();
}
