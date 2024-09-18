package com.thetis.store.hnsw;

import com.stepstone.search.hnswlib.jna.QueryTuple;
import com.stepstone.search.hnswlib.jna.SpaceName;
import com.stepstone.search.hnswlib.jna.exception.QueryCannotReturnResultsException;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.store.Index;
import com.thetis.structures.Id;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class HNSW implements Index<String, Set<String>>
{
    private transient Function<String, List<Double>> embeddingGen;
    private com.stepstone.search.hnswlib.jna.Index hnsw;
    private int embeddingsDim, k;
    private long capacity;
    private EntityLinking linker;
    private EntityTableLink entityTableLink;
    private String indexPath;

    public HNSW(Function<String, List<Double>> embeddingGenerator, int embeddingsDimension, long capacity, int neighborhoodSize,
                EntityLinking linker, EntityTableLink entityTableLink, String indexPath)
    {
        this.embeddingGen = embeddingGenerator;
        this.embeddingsDim = embeddingsDimension;
        this.capacity = capacity;
        this.k = neighborhoodSize;
        this.hnsw = new com.stepstone.search.hnswlib.jna.Index(SpaceName.COSINE, embeddingsDimension);
        this.linker = linker;
        this.entityTableLink = entityTableLink;
        this.indexPath = indexPath;
        this.hnsw.initialize((int) capacity);
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

    public void setCapacity(int capacity)
    {
        this.capacity = capacity;
        this.hnsw.initialize(capacity);
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

        if (embedding != null)
        {
            float[] embeddingsArray = toFloat(embedding);
            this.hnsw.addItem(embeddingsArray, id.getId());
        }
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

        this.hnsw.markDeleted(id.getId());
        return true;
    }

    /**
     * Performs the approximate KNN search over the HNSW index
     * @param key Entity to search for
     * @return Set of tables in which entities that are approximately closest to the key entity in the embedding space exist
     */
    @Override
    public Set<String> find(String key)
    {
        if (this.linker.kgUriLookup(key) == null)
        {
            return Collections.emptySet();
        }

        try
        {
            List<Double> embedding = this.embeddingGen.apply(key);
            float[] primitiveEmbedding = toFloat(embedding);
            QueryTuple results = this.hnsw.knnQuery(primitiveEmbedding, this.k);
            Set<String> tables = new HashSet<>();

            for (int resultId : results.getIds())
            {
                tables.addAll(this.entityTableLink.find(new Id(resultId)));
            }

            return tables;
        }

        catch (QueryCannotReturnResultsException e)
        {
            this.hnsw.setEf(this.hnsw.getEf() * 2);
            return find(key);
        }
    }

    /**
     * Approximately checks whether a given entity exists in the HNSW index
     * Increase K to improve accuracy
     * @param key Entity to check
     * @return True if the entity is in the top-K nearest neighbors
     */
    @Override
    public boolean contains(String key)
    {
        return find(key).stream().anyMatch(key::equals);
    }

    /**
     * Number of entities in the HNSW index
     * @return The size of the HNSW index in terms of number of stored entities
     */
    @Override
    public long size()
    {
        return this.hnsw.getLength();
    }

    /**
     * Clears the HNSW index
     */
    @Override
    public void clear()
    {
        this.hnsw.clear();
    }

    public void save()
    {
        this.hnsw.save(Path.of(this.indexPath));
    }

    public void load()
    {
        this.hnsw.load(Path.of(this.indexPath), (int) this.capacity);
    }
}
