package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * BucketIndex key is RDF type and value is table ID
 */
public class TypesLSHIndex extends BucketIndex<String, String> implements LSHIndex<String, String>, Serializable
{
    private Neo4jEndpoint neo4j;
    private int permutationVectors;
    private double bandFraction;
    private List<List<Integer>> permutations;
    private List<PairNonComparable<String, List<Integer>>> signature;
    private Map<String, Integer> universeTypes;
    private HashFunction hash;

    /**
     * @param neo4j Neo4J driver instance
     * @param permutationVectors Number of permutation vectors used to create min-hash signature (this determines the signature dimension for each entity)
     * @param bandFraction Factor of the dimension each band must make up in size
     * @param tableEntities Set of tables containing its set of entities
     * @param hash A hash function to be applied on min-hash signature to compute bucket index
     * @param bucketCount Number of LSH buckets (this determines runtime and accuracy!)
     */
    public TypesLSHIndex(Neo4jEndpoint neo4j, int permutationVectors, double bandFraction,
                         Set<PairNonComparable<String, Set<String>>> tableEntities, HashFunction hash, int bucketCount)
    {
        super(bucketCount);

        if (bandFraction <= 0 || bandFraction > 1)
        {
            throw new IllegalArgumentException("Band fraction must be greater than zero and no larger than 1");
        }

        this.neo4j = neo4j;
        this.permutationVectors = permutationVectors;
        this.signature = new ArrayList<>();
        this.bandFraction = bandFraction;
        this.hash = hash;
        build(tableEntities);
    }

    private void build(Set<PairNonComparable<String, Set<String>>> tableEntities)
    {
        int counter = 0;
        Set<String> types = this.neo4j.allTypes();
        this.universeTypes = new HashMap<>();

        for (String type : types)
        {
            this.universeTypes.put(type, counter++);
        }

        this.permutations = createPermutations(this.permutationVectors, this.universeTypes.size());

        for (PairNonComparable<String, Set<String>> table : tableEntities)
        {
            String tableName = table.getFirst();
            List<PairNonComparable<String, List<Boolean>>> matrix = new ArrayList<>();  // TODO: Instead of bit matrix, use set of indices of which bits are 1

            for (String entity : table.getSecond())
            {
                List<Boolean> entityBitVector = bitVector(entity);
                matrix.add(new PairNonComparable<>(entity, entityBitVector));
            }

            List<List<Boolean>> pureMatrix = matrix.stream().map(PairNonComparable::getSecond).toList();
            extendSignature(this.signature, matrix, this.permutations);

            for (int entity = 0; entity < pureMatrix.size(); entity++)
            {
                List<Integer> keys = createKeys(entity);

                for (int key : keys)
                {
                    add(key, matrix.get(entity).getFirst(), tableName);
                }
            }
        }
    }

    private List<Boolean> bitVector(String entity)
    {
        Set<String> types = types(entity);
        Set<Integer> typesIndices = new TreeSet<>();    // TreeSet so we don't need to sort the indices when constructing signature
        List<List<Boolean>> bitVectors = new ArrayList<>(types.size());

        for (String type : types)
        {
            List<Boolean> typeBitVector = typeBitVector(type);
            bitVectors.add(typeBitVector);
        }

        return or(bitVectors);
    }

    private Set<String> types(String entity)
    {
        return new HashSet<>(this.neo4j.searchTypes(entity));
    }

    private static List<List<Integer>> createPermutations(int vectors, int dimension)
    {
        List<List<Integer>> permutations = new ArrayList<>();

        for (int i = 0; i < vectors; i++)
        {
            List<Integer> permutation = new ArrayList<>(dimension);
            permutation.addAll(Collections.nCopies(dimension, 0));
            permutations.add(permutation.stream().map(n -> new Random().nextInt(dimension)).collect(Collectors.toList()));
        }

        return permutations;
    }

    private static List<PairNonComparable<String, List<Integer>>> extendSignature(List<PairNonComparable<String, List<Integer>>> signature,
                                                                     List<PairNonComparable<String, List<Boolean>>> entityMatrix,
                                                                     List<List<Integer>> permutations)
    {
        for (PairNonComparable<String, List<Boolean>> entity : entityMatrix)
        {
            int signatureIdx = entitySignatureIndex(signature, entity.getFirst());
            List<Integer> entitySignature;

            if (signatureIdx == -1)
            {
                int entry = entity.getSecond().indexOf(true);

                if (entry == -1)
                {
                    entitySignature = new ArrayList<>(Collections.nCopies(permutations.size(), 0));
                }

                else
                {
                    entitySignature = new ArrayList<>(permutations.size());

                    for (List<Integer> permutation : permutations)
                    {
                        entitySignature.add(permutation.get(entry));
                    }
                }

                signature.add(new PairNonComparable<>(entity.getFirst(), entitySignature));
            }
        }

        return signature;
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

    private List<Boolean> typeBitVector(String typeNode)
    {
        List<Boolean> vector = new ArrayList<>(Collections.nCopies(this.universeTypes.size(), false));

        if (this.universeTypes.containsKey(typeNode))
        {
            vector.set(this.universeTypes.get(typeNode), true);
        }

        return vector;
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
        List<Boolean> entityBitVector = bitVector(entity);
        int entitySignatureIdx = entitySignatureIndex(this.signature, entity);

        if (entitySignatureIdx == -1)
        {
            extendSignature(this.signature, List.of(new PairNonComparable<>(entity, entityBitVector)), this.permutations);
        }

        return entitySignatureIndex(this.signature, entity);
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

    private static List<Boolean> or(List<List<Boolean>> vectors)
    {
        if (vectors.size() == 0)
            return new Vector<>();

        List<Boolean> result = new ArrayList<>();

        for (int i = 0; i < vectors.get(0).size(); i++)
        {
            result.add(false);

            for (List<Boolean> vec : vectors)
            {
                if (vec.get(i))
                {
                    result.set(i, true);
                }
            }
        }

        return result;
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
