package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;
import dk.aau.cs.daisy.edao.structures.graph.Type;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * BucketIndex key is RDF type and value is table ID
 */
public class TypesLSHIndex extends BucketIndex<String, String> implements LSHIndex<String, String>, Serializable
{
    private File neo4jConfFile;
    private int permutationVectors;
    private double bandFraction;
    private List<List<Integer>> permutations;
    private List<PairNonComparable<String, List<Integer>>> signature;
    private Map<String, Integer> universeTypes;
    private HashFunction hash;

    /**
     * @param neo4jConfigFile Neo4J connector configuration file
     * @param permutationVectors Number of permutation vectors used to create min-hash signature (this determines the signature dimension for each entity)
     * @param bandFraction Factor of the dimension each band must make up in size
     * @param tableEntities Set of tables containing its set of entities
     * @param hash A hash function to be applied on min-hash signature to compute bucket index
     * @param bucketCount Number of LSH buckets (this determines runtime and accuracy!)
     */
    public TypesLSHIndex(File neo4jConfigFile, Iterator<Type> entityTypes, int permutationVectors, double bandFraction,
                         Set<PairNonComparable<String, Set<String>>> tableEntities, HashFunction hash, int bucketCount)
    {
        super(bucketCount);

        if (bandFraction <= 0 || bandFraction > 1)
        {
            throw new IllegalArgumentException("Band fraction must be greater than zero and no larger than 1");
        }

        this.neo4jConfFile = neo4jConfigFile;
        this.permutationVectors = permutationVectors;
        this.signature = new ArrayList<>();
        this.bandFraction = bandFraction;
        this.hash = hash;

        loadTypes(entityTypes);

        try
        {
            build(tableEntities);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Could not initialize Neo4J connector");
        }
    }

    private void loadTypes(Iterator<Type> entityTypes)
    {
        int counter = 0;
        this.universeTypes = new HashMap<>();

        while (entityTypes.hasNext())
        {
            String type = entityTypes.next().getType();

            if (!this.universeTypes.containsKey(type))
            {
                this.universeTypes.put(type, counter++);
            }
        }
    }

    /**
     * Instead of storing actual matrix, we only store the smallest index per entity
     * as this is all we need when computing the signature
     */
    // TODO: We can create a thread for each table we iterate
    private void build(Set<PairNonComparable<String, Set<String>>> tableEntities) throws IOException
    {
        Neo4jEndpoint neo4j = new Neo4jEndpoint(this.neo4jConfFile);
        this.permutations = createPermutations(this.permutationVectors, this.universeTypes.size());

        for (PairNonComparable<String, Set<String>> table : tableEntities)
        {
            String tableName = table.getFirst();
            List<PairNonComparable<String, Set<Integer>>> matrix = new ArrayList<>();  // TODO: Instead of bit matrix, use set of indices of which bits are 1

            for (String entity : table.getSecond())
            {
                Set<Integer> entityBitVector = bitVector(entity, neo4j);
                matrix.add(new PairNonComparable<>(entity, entityBitVector));
            }

            extendSignature(this.signature, matrix, this.permutations);

            for (int entity = 0; entity < matrix.size(); entity++)
            {
                List<Integer> keys = createKeys(entity);

                for (int key : keys)
                {
                    add(key, matrix.get(entity).getFirst(), tableName);
                }
            }
        }

        neo4j.close();
    }

    private Set<Integer> bitVector(String entity, Neo4jEndpoint neo4j)
    {
        Set<String> types = types(entity, neo4j);
        Set<Integer> indices = new HashSet<>(types.size());

        for (String type : types)
        {
            if (this.universeTypes.containsKey(type))
            {
                indices.add(this.universeTypes.get(type));
            }
        }

        return indices;
    }

    private Set<String> types(String entity, Neo4jEndpoint neo4j)
    {
        return new HashSet<>(neo4j.searchTypes(entity));   // We could also use the EntityTable index here
    }

    private static List<List<Integer>> createPermutations(int vectors, int dimension)
    {
        List<List<Integer>> permutations = new ArrayList<>();
        Set<Integer> indices = new HashSet<>(dimension);

        for (int i = 0; i < dimension; i++)
        {
            indices.add(i);
        }

        for (int i = 0; i < vectors; i++)
        {
            List<Integer> permutation = new ArrayList<>(dimension);
            List<Integer> indicesCopy = new ArrayList<>(indices);

            while (!indicesCopy.isEmpty())
            {
                permutation.add(indicesCopy.remove(new Random(indicesCopy.size()).nextInt()));
            }

            permutations.add(permutation);
        }

        return permutations;
    }

    private static List<PairNonComparable<String, List<Integer>>> extendSignature(List<PairNonComparable<String, List<Integer>>> signature,
                                                                     List<PairNonComparable<String, Set<Integer>>> entityMatrix,
                                                                     List<List<Integer>> permutations)
    {
        for (PairNonComparable<String, Set<Integer>> entity : entityMatrix)
        {
            int signatureIdx = entitySignatureIndex(signature, entity.getFirst());
            List<Integer> entitySignature;

            if (signatureIdx == -1)
            {
                Set<Integer> bitVector = entity.getSecond();

                if (bitVector.isEmpty())
                {
                    entitySignature = new ArrayList<>(Collections.nCopies(permutations.size(), 0));
                }

                else
                {
                    entitySignature = new ArrayList<>(permutations.size());

                    for (List<Integer> permutation : permutations)
                    {
                        int reArrangedMin = reArrangeMin(bitVector, permutation);
                        entitySignature.add(permutation.get(reArrangedMin));
                    }
                }

                signature.add(new PairNonComparable<>(entity.getFirst(), entitySignature));
            }
        }

        return signature;
    }

    private static int reArrangeMin(Set<Integer> bitVector, List<Integer> permutation)
    {
        Iterator<Integer> iter = bitVector.iterator();
        int smallest = Integer.MAX_VALUE;

        while (iter.hasNext())
        {
            int idx = iter.next();
            int permuted = permutation.get(idx);

            if (permuted < smallest)
            {
                smallest = permuted;
            }
        }

        return smallest;
    }

    private static int entitySignatureIndex(List<PairNonComparable<String, List<Integer>>> signature, String entity)
    {
        int signatures = signature.size();

        for (int i = 0; i < signatures; i++)
        {
            if (signature.get(i).getFirst().equals(entity))
            {
                return i;
            }
        }

        return -1;
    }

    private List<Integer> createKeys(int signatureIndex)
    {
        int bandSize = (int) Math.floor(this.permutations.size() * this.bandFraction);
        int permutationSize = this.permutations.size();
        List<Integer> keys = new ArrayList<>();

        for (int idx = 0; idx < permutationSize; idx += bandSize)
        {
            int bandEnd = Math.min(idx + bandSize, permutationSize);
            List<Integer> subSignature = this.signature.get(signatureIndex).getSecond().subList(idx, bandEnd);
            int key = Math.abs(this.hash.hash(subSignature, buckets()));
            keys.add(key);
        }

        return keys;
    }

    private int createOrGetSignature(String entity)
    {
        try (Neo4jEndpoint neo4j = new Neo4jEndpoint(this.neo4jConfFile))
        {
            Set<Integer> entityBitVector = bitVector(entity, neo4j);
            int entitySignatureIdx = entitySignatureIndex(this.signature, entity);


            if (entitySignatureIdx == -1)
            {
                extendSignature(this.signature, List.of(new PairNonComparable<>(entity, entityBitVector)), this.permutations);
            }

            return entitySignatureIndex(this.signature, entity);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed initializing Neo4J connector");
        }
    }

    /**
     * Insert single entity into LSH index
     * @param entity Entity to be inserted
     * @param table Table in which the entity exists
     * @return True if the entity could be inserted
     */
    @Override
    public boolean insert(String entity, String table)
    {
        try
        {
            int entitySignature = createOrGetSignature(entity);
            List<Integer> bucketKeys = createKeys(entitySignature);

            for (int bucketKey : bucketKeys)
            {
                add(bucketKey, entity, table);
            }

            return true;
        }

        catch (Exception exc)
        {
            return false;
        }
    }

    /**
     * Finds buckets of similar entities and returns tables contained
     * @param entity Query entity
     * @return Set of tables
     */
    @Override
    public Set<String> search(String entity)
    {
        Set<String> candidateTables = new HashSet<>();
        int entitySignatureIdx = createOrGetSignature(entity);
        List<Integer> keys = createKeys(entitySignatureIdx);

        for (int key : keys)
        {
            candidateTables.addAll(get(key));
        }

        return candidateTables;
    }
}
