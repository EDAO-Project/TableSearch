package dk.aau.cs.daisy.edao.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EmbeddingsIndex<C> implements ClusteredIndex<C, String, List<Double>>, Serializable
{
    private final Map<String, List<Double>> embeddingsMap = new ConcurrentHashMap<>();
    private Map<C, Map<String, List<Double>>> clusteredEmbeddings = new ConcurrentHashMap<>();

    @Override
    public void insert(String key, List<Double> value)
    {
        this.embeddingsMap.put(key, value);
    }

    @Override
    public boolean remove(String key)
    {
        return this.embeddingsMap.remove(key) != null;
    }

    @Override
    public List<Double> find(String key)
    {
        return this.embeddingsMap.get(key);
    }

    @Override
    public boolean contains(String key)
    {
        return this.embeddingsMap.containsKey(key);
    }

    @Override
    public int size()
    {
        return this.embeddingsMap.size();
    }

    @Override
    public void clear()
    {
        this.embeddingsMap.clear();
    }

    @Override
    public void clusterInsert(C cluster, String key, List<Double> value)
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
    public List<Double> clusterGet(C cluster, String key)
    {
        if (this.clusteredEmbeddings.containsKey(cluster))
        {
            return this.clusteredEmbeddings.get(cluster).get(key);
        }

        return null;
    }
}
