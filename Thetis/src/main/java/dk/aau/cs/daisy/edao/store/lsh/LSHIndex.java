package dk.aau.cs.daisy.edao.store.lsh;

import java.util.List;
import java.util.Set;

public interface LSHIndex<K, V>
{
    boolean insert(K key, V value);
    Set<V> search(K key);
    Set<V> search(K key, int vote);
    int size();
}
