package com.thetis.store;

import com.thetis.structures.Id;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EmbeddingsIndex<C> implements ClusteredIndex<C, Id, List<Double>>, Serializable
{
    private final Map<Id, List<Double>> embeddingsMap = new ConcurrentHashMap<>();
    private Map<C, Map<Id, List<Double>>> clusteredEmbeddings = new ConcurrentHashMap<>();

    @Override
    public void insert(Id key, List<Double> value)
    {
        this.embeddingsMap.put(key, value);
    }

    @Override
    public boolean remove(Id key)
    {
        return this.embeddingsMap.remove(key) != null;
    }

    @Override
    public List<Double> find(Id key)
    {
        return this.embeddingsMap.get(key);
    }

    @Override
    public boolean contains(Id key)
    {
        return this.embeddingsMap.containsKey(key);
    }

    @Override
    public long size()
    {
        return this.embeddingsMap.size();
    }

    @Override
    public void clear()
    {
        this.embeddingsMap.clear();
    }

    @Override
    public void clusterInsert(C cluster, Id key, List<Double> value)
    {
        if (!this.clusteredEmbeddings.containsKey(cluster))
        {
            this.clusteredEmbeddings.put(cluster, new HashMap<>());
        }

        this.clusteredEmbeddings.get(cluster).put(key, value);
    }

    @Override
    public boolean clusterRemove(C cluster)
    {
        return this.clusteredEmbeddings.remove(cluster) != null;
    }

    @Override
    public boolean containsCluster(C cluster)
    {
        return this.clusteredEmbeddings.containsKey(cluster);
    }

    @Override
    public int clusters()
    {
        return this.clusteredEmbeddings.size();
    }

    @Override
    public void dropClusters()
    {
        this.clusteredEmbeddings.clear();
    }

    @Override
    public List<Double> clusterGet(C cluster, Id key)
    {
        if (this.clusteredEmbeddings.containsKey(cluster))
        {
            return this.clusteredEmbeddings.get(cluster).get(key);
        }

        return null;
    }
}
