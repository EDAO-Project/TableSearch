package com.thetis.store.lsh;

import com.thetis.store.Index;

import java.util.Set;

public interface LSHIndex<K, V> extends Index<K, V>
{
    Set<V> search(K key);
    Set<V> search(K key, int vote);
    Set<V> agggregatedSearch(K ... keys);
    Set<V> agggregatedSearch(int vote, K ... keys);
}
