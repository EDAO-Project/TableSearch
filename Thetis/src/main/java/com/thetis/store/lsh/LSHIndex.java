package com.thetis.store.lsh;

import java.util.List;
import java.util.Set;

public interface LSHIndex<K, V>
{
    boolean insert(K key, V value);
    Set<V> search(K key);
    Set<V> search(K key, int vote);
    Set<V> agggregatedSearch(K ... keys);
    Set<V> agggregatedSearch(int vote, K ... keys);
    int size();
}
