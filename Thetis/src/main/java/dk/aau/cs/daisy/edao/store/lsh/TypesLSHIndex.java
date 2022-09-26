package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;
import dk.aau.cs.daisy.edao.structures.graph.Type;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * BucketIndex key is RDF type and value is table ID
 */
public class TypesLSHIndex extends BucketIndex<Id, String> implements LSHIndex<String, String>, Serializable
{
    private File neo4jConfFile;
    private int shingles;
    private int permutationVectors;
    private double bandFraction;
    private List<List<Integer>> permutations;
    private List<PairNonComparable<Id, List<Integer>>> signature;
    private Map<String, Integer> universeTypes;
    private HashFunction hash;
    private transient int threads;
    private transient final Object lock = new Object();
    private transient EntityLinking linker = null;
    private final Map<Id, Integer> entityToSigIndex = new HashMap<>();
    private Set<String> unimportantTypes;
    private static final double UNIMPORTANT_PERCENTILE = 0.9;

    /**
     * @param neo4jConfigFile Neo4J connector configuration file
     * @param permutationVectors Number of permutation vectors used to create min-hash signature (this determines the signature dimension for each entity)
     * @param bandFraction Factor of the dimension each band must make up in size
     * @param tableEntities Set of tables containing its set of entities
     * @param hash A hash function to be applied on min-hash signature to compute bucket index
     * @param bucketCount Number of LSH buckets (this determines runtime and accuracy!)
     */
    public TypesLSHIndex(File neo4jConfigFile, int permutationVectors, double bandFraction, int shingleSize,
                         Set<PairNonComparable<String, Set<String>>> tableEntities, HashFunction hash, int bucketCount,
                         int threads, EntityLinking linker, EntityTable entityTable)
    {
        super(bucketCount);

        if (bandFraction <= 0 || bandFraction > 1)
        {
            throw new IllegalArgumentException("Band fraction must be greater than zero and no larger than 1");
        }

        else if (shingleSize <= 0)
        {
            throw new IllegalArgumentException("Shingle size must be greater than 0");
        }

        this.neo4jConfFile = neo4jConfigFile;
        this.shingles = shingleSize;
        this.permutationVectors = permutationVectors;
        this.signature = new ArrayList<>();
        this.bandFraction = bandFraction;
        this.hash = hash;
        this.threads = threads;
        this.linker = linker;

        loadTypes(entityTable);

        try
        {
            build(tableEntities);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Could not initialize Neo4J connector: " + e.getMessage());
        }
    }

    public void useEntityLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    private void loadTypes(EntityTable entityTable)
    {
        int counter = 0;
        this.universeTypes = new HashMap<>();
        Iterator<Type> types = entityTable.allTypes();

        while (types.hasNext())
        {
            String type = types.next().getType();

            if (!this.universeTypes.containsKey(type))
            {
                this.universeTypes.put(type, counter++);
            }
        }

        this.unimportantTypes = new TypeStats(entityTable).popularByPercentile(UNIMPORTANT_PERCENTILE);
    }

    /**
     * Instead of storing actual matrix, we only store the smallest index per entity
     * as this is all we need when computing the signature
     */
    private void build(Set<PairNonComparable<String, Set<String>>> tableEntities) throws IOException
    {
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        ExecutorService executor = Executors.newFixedThreadPool(this.threads);
        List<Future<?>> futures = new ArrayList<>(tableEntities.size());
        Neo4jEndpoint neo4j = new Neo4jEndpoint(this.neo4jConfFile);
        int typesDimension = this.universeTypes.size();

        for (int i = 1; i < this.shingles; i++)
        {
            typesDimension = concat(typesDimension, this.universeTypes.size());
        }

        this.permutations = createPermutations(this.permutationVectors, ++typesDimension);

        for (PairNonComparable<String, Set<String>> table : tableEntities)
        {
            futures.add(executor.submit(() -> loadTable(table, neo4j)));
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

        neo4j.close();
    }

    private void loadTable(PairNonComparable<String, Set<String>> table, Neo4jEndpoint neo4j)
    {
        String tableName = table.getFirst();
        List<PairNonComparable<Id, Set<Integer>>> matrix = new ArrayList<>();

        for (String entity : table.getSecond())
        {
            Id entityId = this.linker.kgUriLookup(entity);
            Set<Integer> entityBitVector = bitVector(entity, neo4j);
            matrix.add(new PairNonComparable<>(entityId, entityBitVector));
        }

        synchronized (this.lock)
        {
            extendSignature(this.signature, matrix, this.permutations, this.entityToSigIndex);
        }

        for (int entity = 0; entity < matrix.size(); entity++)
        {
            List<Integer> keys = createKeys(entity);
            Id entityId = matrix.get(entity).getFirst();

            for (int key : keys)
            {
                synchronized (this.lock)
                {
                    add(key, entityId, tableName);
                }
            }
        }
    }

    private Set<Integer> bitVector(String entity, Neo4jEndpoint neo4j)
    {
        List<String> types = types(entity, neo4j);
        int typesCount = types.size();
        Set<Integer> indices = new HashSet<>(types.size());
        List<Integer> shingle = new ArrayList<>(this.shingles);

        for (int i = 0; i < typesCount; i++)
        {
            String type = types.get(i);

            if (!this.unimportantTypes.contains(type) && this.universeTypes.containsKey(type))
            {
                shingle.add(this.universeTypes.get(type));

                if (shingle.size() == this.shingles)
                {
                    int concatenated = shingle.get(0);

                    for (int j = 1; j < shingle.size(); j++)
                    {
                        concatenated = concat(concatenated, shingle.get(j));
                    }

                    indices.add(concatenated);
                    shingle.clear();
                    i -= this.shingles - 1;
                }
            }
        }

        return indices;
    }

    private synchronized List<String> types(String entity, Neo4jEndpoint neo4j)
    {
        return neo4j.searchTypes(entity);   // Type ordering is important because of n-gram computation
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
                int idx = new Random().nextInt(indicesCopy.size());
                permutation.add(indicesCopy.remove(idx));
            }

            permutations.add(permutation);
        }

        return permutations;
    }

    private static List<PairNonComparable<Id, List<Integer>>> extendSignature(List<PairNonComparable<Id, List<Integer>>> signature,
                                                                     List<PairNonComparable<Id, Set<Integer>>> entityMatrix,
                                                                     List<List<Integer>> permutations,
                                                                     Map<Id, Integer> entityToSigIdx)
    {
        for (PairNonComparable<Id, Set<Integer>> entity : entityMatrix)
        {
            List<Integer> entitySignature;
            Id entityId = entity.getFirst();

            if (!entityToSigIdx.containsKey(entityId))
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
                entityToSigIdx.put(entityId, signature.size() - 1);
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

        return permutation.get(smallest);
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
            int key = Math.abs(this.hash.hash(subSignature, size()));
            keys.add(key);
        }

        return keys;
    }

    private int createOrGetSignature(String entity)
    {
        Id entityId = this.linker.kgUriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in EntityLinker object");
        }

        try (Neo4jEndpoint neo4j = new Neo4jEndpoint(this.neo4jConfFile))
        {
            Set<Integer> entityBitVector = bitVector(entity, neo4j);
            extendSignature(this.signature, List.of(new PairNonComparable<>(entityId, entityBitVector)),
                    this.permutations, this.entityToSigIndex);

            return this.entityToSigIndex.get(entityId);
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
        if (this.linker == null)
        {
            throw new RuntimeException("No EntityLinker object has been specified");
        }

        Id entityId = this.linker.kgUriLookup(entity);

        if (entityId == null)
        {
            throw new RuntimeException("Entity does not exist in EntityLinker object");
        }

        try
        {
            int entitySignature = createOrGetSignature(entity);
            List<Integer> bucketKeys = createKeys(entitySignature);

            for (int bucketKey : bucketKeys)
            {
                add(bucketKey, entityId, table);
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

    private static int concat(int a, int b)
    {
        return (int) (b + a * Math.pow(10, Math.ceil(Math.log10(b + 1))));
    }
}
