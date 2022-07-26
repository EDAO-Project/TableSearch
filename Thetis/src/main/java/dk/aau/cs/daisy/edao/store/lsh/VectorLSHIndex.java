package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.Factory;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;

import java.io.Serializable;
import java.util.*;

/**
 * LSH index of entity embeddings
 * Mapping from string entity to set of tables candidate entities by cosine similarity originate from
 */
public class VectorLSHIndex extends BucketIndex<String, String> implements LSHIndex<PairNonComparable<String, List<Double>>, String>, Serializable
{
    private HashFunction hash;
    private Set<List<Double>> projections;
    private List<List<Boolean>> bucketSignatures;

    /**
     * @param bucketCount Number of LSH index buckets
     * @param projections Number of projections, which determines hash size
     * @param hash Hash function to be applied on min-hash signatures
     * @param tableVectors Tables containing entities and their vector (embedding) representations
     */
    public VectorLSHIndex(int bucketCount, int projections, HashFunction hash,
                          Set<PairNonComparable<String, Set<String>>> tableVectors)
    {
        super(bucketCount);
        this.hash = hash;
        this.bucketSignatures = new ArrayList<>(Collections.nCopies(bucketCount, null));
        load(tableVectors, projections);
    }

    private void load(Set<PairNonComparable<String, Set<String>>> tableVectors, int projections)
    {
        Set<PairNonComparable<String, Set<PairNonComparable<String, List<Double>>>>> materialized = new HashSet<>();
        DBDriver<List<Double>, String> embeddingsDB = Factory.fromConfig(false);

        for (PairNonComparable<String, Set<String>> table : tableVectors)
        {
            String tableName = table.getFirst();
            Set<PairNonComparable<String, List<Double>>> entities = new HashSet<>(table.getSecond().size());

            for (String entity : table.getSecond())
            {
                List<Double> embedding = embeddingsDB.select(entity);

                if (embedding != null)
                {
                    entities.add(new PairNonComparable<>(entity, embedding));
                }
            }

            materialized.add(new PairNonComparable<>(tableName, entities));
        }

        if (materialized.isEmpty())
        {
            throw new RuntimeException("No embeddings were loaded into vector LSH index");
        }

        this.projections = createProjections(projections, getVectorDimension(materialized));

        for (PairNonComparable<String, Set<PairNonComparable<String, List<Double>>>> table : materialized)
        {
            String tableName = table.getFirst();

            for (PairNonComparable<String, List<Double>> entity : table.getSecond())
            {
                String uri = entity.getFirst();
                List<Boolean> bitVector = bitVector(entity.getSecond());
                int key = this.hash.hash(bitVector, buckets());
                add(key, uri, tableName);
                this.bucketSignatures.set(key, bitVector);  // Do we need to set this multiple times for each key when two have the same key?
            }
        }
    }

    // This assumes all vectors are of the same dimension
    private static int getVectorDimension(Set<PairNonComparable<String, Set<PairNonComparable<String, List<Double>>>>> tables)
    {
        int dimension = -1;

        for (PairNonComparable<String, Set<PairNonComparable<String, List<Double>>>> table : tables)
        {
            for (PairNonComparable<String, List<Double>> entity : table.getSecond())
            {
                dimension = entity.getSecond().size();
                break;
            }

            if (dimension != -1)
            {
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
    public boolean insert(PairNonComparable<String, List<Double>> entity, String table)
    {
        List<Boolean> bitVector = bitVector(entity.getSecond());
        int key = this.hash.hash(bitVector, buckets());
        add(key, entity.getFirst(), table);

        return true;
    }

    @Override
    public Set<String> search(PairNonComparable<String, List<Double>> entity)
    {
        List<Boolean> searchBitVector = bitVector(entity.getSecond());
        int buckets = buckets(), closestIdx = 0, closestDist = Integer.MAX_VALUE;

        for (int i = 1; i < buckets; i++)
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
