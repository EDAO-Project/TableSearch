package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.Factory;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
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
    private List<List<Boolean>> bucketSignatures;
    private transient int threads;
    private transient final Object lock = new Object();
    private transient EntityLinking linker = null;

    /**
     * @param bucketCount Number of LSH index buckets
     * @param projections Number of projections, which determines hash size
     * @param tableVectors Tables containing entities and their vector (embedding) representations
     */
    public VectorLSHIndex(int bucketCount, int projections, Set<PairNonComparable<String, Set<String>>> tableVectors,
                          int threads, EntityLinking linker)
    {
        super(bucketCount);
        this.bucketSignatures = new ArrayList<>(Collections.nCopies(bucketCount, null));
        this.threads = threads;
        this.linker = linker;
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

            synchronized (this.lock)
            {
                embedding = embeddingsDB.select(entity);

                if (embedding == null)
                {
                    continue;
                }
            }

            List<Boolean> bitVector = bitVector(embedding);
            int key = key(Integer.parseInt(toBitString(bitVector), 2), size());

            synchronized (this.lock)
            {
                add(key, entityId, tableName);
                this.bucketSignatures.set(key, bitVector);
            }
        }
    }

    private static String toBitString(List<Boolean> bitVector)
    {
        StringBuilder builder = new StringBuilder();
        bitVector.forEach(b -> {
            if (b)
            {
                builder.append("1");
            }

            else
            {
                builder.append("0");
            }
        });

        return builder.toString();
    }

    private static int key(int value, int size)
    {
        return value % size;
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

    private List<Boolean> bitVector(List<Double> vector)
    {
        List<Boolean> bitVector = new ArrayList<>(this.projections.size());

        for (List<Double> projection : this.projections)
        {
            double dotProduct = dot(projection, vector);
            bitVector.add(dotProduct > 0);
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

        List<Boolean> bitVector = bitVector(embedding);
        int key = key(Integer.parseInt(toBitString(bitVector), 2), size());
        add(key, entityId, table);
        this.bucketSignatures.set(key, bitVector);

        return true;
    }

    @Override
    public Set<String> search(String entity)
    {
        DBDriver<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
        List<Double> embedding = embeddingsDB.select(entity);
        embeddingsDB.close();

        if (embedding == null)
        {
            return new HashSet<>();
        }

        List<Boolean> searchBitVector = bitVector(embedding);
        int buckets = size(), closestIdx = 0, closestDist = Integer.MAX_VALUE;

        for (int i = 0; i < buckets; i++)
        {
            if (this.bucketSignatures.get(i) == null)
            {
                continue;
            }

            int distance = (int) new HammingDistance<>(searchBitVector, this.bucketSignatures.get(i)).distance();

            if (distance < closestDist)
            {
                closestDist = distance;
                closestIdx = i;
            }
        }

        return get(closestIdx);
    }
}
