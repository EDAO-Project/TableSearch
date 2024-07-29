package com.thetis.store.hnsw;

import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.store.Index;
import com.thetis.structures.Id;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class HNSW implements Index<String, Set<String>>
{
    private transient Function<String, List<Double>> embeddingGen;
    private cloud.unum.usearch.Index hnsw;
    private int embeddingsDim, k;
    private long capacity;
    private EntityLinking linker;
    private EntityTableLink entityTableLink;
    private String indexPath;

    public HNSW(Function<String, List<Double>> embeddingGenerator, int embeddingsDimension, long capacity, int neighborhoodSize,
                EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, String indexPath)
    {
        this.embeddingGen = embeddingGenerator;
        this.embeddingsDim = embeddingsDimension;
        this.capacity = capacity;
        this.k = neighborhoodSize;
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(embeddingsDimension).build();
        this.linker = linker;
        this.entityTableLink = entityTableLink;
        this.indexPath = indexPath;
        this.hnsw.reserve(capacity);
    }

    public void setLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    public void setEntityTableLink(EntityTableLink entityTableLink)
    {
        this.entityTableLink = entityTableLink;
    }

    public void setEmbeddingGenerator(Function<String, List<Double>> embeddingGenerator)
    {
        this.embeddingGen = embeddingGenerator;
    }

    public int getEmbeddingsDimension()
    {
        return this.embeddingsDim;
    }

    public long getCapacity()
    {
        return this.capacity;
    }

    public int getNeighborhoodSize()
    {
        return this.k;
    }

    public String getIndexPath()
    {
        return this.indexPath;
    }

    private static float[] toFloat(List<Double> embedding)
    {
        float[] embeddingsArray = new float[embedding.size()];
        int i = 0;

        for (double e : embedding)
        {
            embeddingsArray[i++] = (float) e;
        }

        return embeddingsArray;
    }

    public void setCapacity(long capacity)
    {
        this.capacity = capacity;
        this.hnsw.reserve(this.capacity);
    }

    public void setK(int k)
    {
        this.k = k;
    }

    /**
     * Inserts an entry into the HSNW index that maps an entity to a set of tables
     * If the mapping already exists, the tables will be added to the co-domain of the mapping
     * @param key Entity to be inserted
     * @param tables Tables to be inserted into the index and mapped to by the given entity (ignore this parameter since we retrieve the tables from another index)
     */
    @Override
    public void insert(String key, Set<String> tables)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return;
        }

        List<Double> embedding = this.embeddingGen.apply(key);
        float[] embeddingsArray = toFloat(embedding);

        this.hnsw.add(id.getId(), embeddingsArray);
    }

    /**
     * Removes the entity from the HNSW index
     * @param key Entity to be removed
     * @return True if the entity has an ID and thereby can be removed, otherwise false
     */
    @Override
    public boolean remove(String key)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return false;
        }

        this.hnsw.remove(id.getId());
        return true;
    }

    @Override
    public Set<String> find(String key)
    {
        if (this.linker.kgUriLookup(key) == null)
        {
            return Collections.emptySet();
        }

        List<Double> embedding = this.embeddingGen.apply(key);
        float[] primitiveEmbedding = toFloat(embedding);
        int[] ids = this.hnsw.search(primitiveEmbedding, this.k);
        Set<String> tables = new HashSet<>();

        for (int resultId : ids)
        {
            tables.addAll(this.entityTableLink.find(new Id(resultId)));
        }

        return tables;
    }

    /**
     * Approximately checks whether a given entity exists in the HNSW index
     * @param key Entity to check
     * @return True if the entity is in the top-K nearest neighbors
     */
    @Override
    public boolean contains(String key)
    {
        if (this.linker.kgUriLookup(key) == null)
        {
            return false;
        }

        List<Double> embedding = this.embeddingGen.apply(key);
        float[] primitiveEmbedding = toFloat(embedding);
        int[] ids = this.hnsw.search(primitiveEmbedding, this.k);

        for (int resultId : ids)
        {
            if (this.linker.kgUriLookup(new Id(resultId)) != null)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Number of entities in the HNSW index
     * @return The size of the HNSW index in terms of number of stored entities
     */
    @Override
    public long size()
    {
        return this.hnsw.size();
    }

    /**
     * Clears the HNSW index
     */
    @Override
    public void clear()
    {
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(this.embeddingsDim).build();
    }

    public void save()
    {
        this.hnsw.save(this.indexPath);
    }

    public void load()
    {
        this.hnsw = new cloud.unum.usearch.Index.Config().metric("cos").dimensions(this.embeddingsDim).build();
        this.hnsw.reserve(this.capacity);
        this.hnsw.load(this.indexPath);
    }
}
