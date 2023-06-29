package com.thetis.store;

public interface ClusteredIndex<C, K, V> extends Index<K, V>
{
    void clusterInsert(C cluster, K key, V value);
    boolean clusterRemove(C cluster);
    boolean containsCluster(C cluster);
    int clusters();
    void dropClusters();
    V clusterGet(C cluster, K key);
}
