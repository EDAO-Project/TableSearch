package dk.aau.cs.daisy.edao.store.lsh;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.Factory;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * LSH index of entity embeddings
 * Mapping from string entity to set of tables candidate entities by cosine similarity originate from
 */
public class VectorLSHIndex extends BucketIndex<Id, String> implements LSHIndex<String, String>, Serializable
{
    private Set<List<Double>> projections;
    private int bandSize;
    private transient int threads;
    private transient final Object lock = new Object();
    private transient EntityLinking linker = null;
    private HashFunction hash;
    private transient Cache<Id, List<Integer>> cache;

    /**
     * @param bucketCount Number of LSH index buckets
     * @param projections Number of projections, which determines hash size
     * @param tableVectors Tables containing entities and their vector (embedding) representations
     * @param hash Hash function applied to bit vector representations of entities
     */
    public VectorLSHIndex(int bucketGroups, int bucketCount, int projections, int bandSize,
                          Set<PairNonComparable<String, Set<String>>> tableVectors, int threads, EntityLinking linker,
                          HashFunction hash)
    {
        super(bucketGroups, bucketCount);
        this.bandSize = bandSize;
        this.threads = threads;
        this.linker = linker;
        this.hash = hash;
        this.cache = CacheBuilder.newBuilder().maximumSize(500).build();
        load(tableVectors, projections);
    }

    public void useEntityLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    private void load(Set<PairNonComparable<String, Set<String>>> tableVectors, int projections)
    {
        DBDriver<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
        ExecutorService executor = Executors.newFixedThreadPool(this.threads);
        List<Future<?>> futures = new ArrayList<>(tableVectors.size());

        if (tableVectors.isEmpty())
        {
            throw new RuntimeException("No tables to load LSH index of embeddings");
        }

        int dimension = embeddingsDimension(tableVectors.iterator().next().getSecond(), embeddingsDB);
        this.projections = createProjections(projections, dimension);

        for (PairNonComparable<String, Set<String>> table : tableVectors)
        {
            futures.add(executor.submit(() -> loadTable(table, embeddingsDB)));
        }

        try
        {
            for (Future<?> f : futures)
            {
                f.get();
            }
        }

        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException("Error in multi-threaded loading of LSH index: " + e.getMessage());
        }

        embeddingsDB.close();
    }

    private void loadTable(PairNonComparable<String, Set<String>> table, DBDriver<List<Double>, String> embeddingsDB)
    {
        String tableName = table.getFirst();

        for (String entity : table.getSecond())
        {
            List<Double> embedding;
            Id entityId = this.linker.kgUriLookup(entity);
            List<Integer> keys;

            if ((keys = this.cache.getIfPresent(entityId)) != null)
            {
                insertEntity(entityId, keys, null, tableName, false);
                continue;
            }

            synchronized (this.lock)
            {
                embedding = embeddingsDB.select(entity);

                if (embedding == null)
                {
                    continue;
                }
            }

            List<Integer> bitVector = bitVector(embedding);
            keys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
            this.cache.put(entityId, keys);
            insertEntity(entityId, keys, bitVector, tableName, true);
        }
    }

    private void insertEntity(Id entityId, List<Integer> keys, List<Integer> bitVector, String tableName, boolean updateBucketSignatures)
    {
        for (int group = 0; group < keys.size(); group++)
        {
            synchronized (this.lock)
            {
                add(group, keys.get(group), entityId, tableName);
            }
        }
    }

    private int embeddingsDimension(Set<String> entities, DBDriver<List<Double>, String> embeddingsDB)
    {
        int dimension = -1;

        for (String entity : entities)
        {
            List<Double> embedding = embeddingsDB.select(entity);

            if (embedding != null && !embedding.isEmpty())
            {
                dimension = embedding.size();
                break;
            }
        }

        return dimension;
    }

    private static Set<List<Double>> createProjections(int num, int dimension)
    {
        Set<List<Double>> projections = new HashSet<>();
        Random r = new Random();
        double min = -1.0, max = 1.0;

        for (int i = 0; i < num; i++)
        {
            List<Double> projection = new ArrayList<>(dimension);

            for (int dim = 0; dim < dimension; dim++)
            {
                projection.add(min + (max - min) * r.nextDouble());
            }

            projections.add(projection);
        }

        return projections;
    }

    private static double dot(List<Double> v1, List<Double> v2)
    {
        if (v1.size() != v2.size())
        {
            throw new IllegalArgumentException("Vectors are not of the same dimension");
        }

        double product = 0;

        for (int i = 0; i < v1.size(); i++)
        {
            product += v1.get(i) * v2.get(i);
        }

        return product;
    }

    private List<Integer> bitVector(List<Double> vector)
    {
        List<Integer> bitVector = new ArrayList<>(this.projections.size());

        for (List<Double> projection : this.projections)
        {
            double dotProduct = dot(projection, vector);
            bitVector.add(dotProduct > 0 ? 1 : 0);
        }

        return bitVector;
    }

    @Override
    public boolean insert(String entity, String table)
    {
        if (this.linker == null)
        {
            throw new RuntimeException("Missing EntityLinker object");
        }

        Id entityId = this.linker.kgUriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in specified EntityLinker object");
        }

        DBDriver<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
        List<Double> embedding = embeddingsDB.select(entity);
        embeddingsDB.close();

        if (embedding == null)
        {
            return false;
        }

        List<Integer> bitVector = bitVector(embedding);
        List<Integer> keys = createKeys(this.projections.size(), this.bandSize, bitVector, groupSize(), this.hash);
        insertEntity(entityId, keys, bitVector, table, true);

        return true;
    }

    @Override
    public Set<String> search(String entity)
    {
        Set<String> tables = new HashSet<>();
        DBDriver<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
        List<Double> embedding = embeddingsDB.select(entity);
        embeddingsDB.close();

        if (embedding == null)
        {
            return new HashSet<>();
        }

        List<Integer> searchBitVector = bitVector(embedding);
        List<Integer> keys = createKeys(this.projections.size(), this.bandSize, searchBitVector, groupSize(), this.hash);

        for (int group = 0; group < keys.size(); group++)
        {
            tables.addAll(get(group, keys.get(group)));
        }

        return tables;
    }
}
