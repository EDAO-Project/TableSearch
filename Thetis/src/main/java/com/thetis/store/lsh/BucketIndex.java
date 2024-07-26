package com.thetis.store.lsh;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BucketIndex<K, V> implements Serializable
{
    private List<BucketGroup<K, V>> groups;

    protected BucketIndex(int groups, int groupBuckets)
    {
        this.groups = new ArrayList<>(groups);

        for (int i = 0; i < groups; i++)
        {
            this.groups.add(new BucketGroup<>(groupBuckets));
        }
    }

    /**
     * Getter to list of all bucket groups
     * @return All bucket groups
     */
    public List<BucketGroup<K, V>> bucketGroups()
    {
        return this.groups;
    }

    /**
     * Getter to number of bucket groups
     * @return Number of bucket groups
     */
    public long size()
    {
        return this.groups.size();
    }

    /**
     * Getter to size of each bucket group
     * @return Size of each bucket group
     */
    public int groupSize()
    {
        return this.groups.get(0).size();
    }

    /**
     * Add key-value pair to bucket bucket group
     * @param group Group containing bucket to be populated
     * @param bucketIndex Index of bucket within group to be populated
     * @param key Key from key-value pair to be added
     * @param value Value from key-value pair to be added
     */
    protected void add(int group, int bucketIndex, K key, V value)
    {
        this.groups.get(group).add(bucketIndex, key, value);
    }

    /**
     * Getter to set of values in bucket from a specific bucket group
     * @param group Index of bucket group with bucket of interest
     * @param bucketIndex Bucket of interest within bucket group
     * @return All values in bucket in bucket group
     */
    protected Set<V> get(int group, int bucketIndex)
    {
        return this.groups.get(group).get(bucketIndex);
    }

    @Override
    public String toString()
    {
        return this.groups.toString();
    }

    /**
     * Creates keys from bands for each bucket group
     * @return List of keys, one for each bucket group
     */
    protected static List<Integer> createKeys(int permutations, int bandSize, List<Integer> signature,
                                              int bucketGroupSize, HashFunction hash)
    {
        List<Integer> keys = new ArrayList<>();

        for (int idx = 0; idx < permutations; idx += bandSize)
        {
            int bandEnd = Math.min(idx + bandSize, permutations);
            List<Integer> subSignature = signature.subList(idx, bandEnd);
            int key = Math.abs(hash.hash(subSignature, bucketGroupSize));
            keys.add(key);
        }

        return keys;
    }

    protected Set<V> search(List<Integer> keys, int vote)
    {
        Map<V, Integer> occurrences = new HashMap<>();

        for (int group = 0; group < keys.size(); group++)
        {
            Set<V> bucketTables = get(group, keys.get(group));
            bucketTables.forEach(t -> occurrences.put(t, occurrences.containsKey(t) ? occurrences.get(t) + 1 : 1));
        }

        return occurrences.entrySet().stream()
                .filter(e -> e.getValue() >= vote)
                .map(Map.Entry::getKey).collect(Collectors.toSet());
    }
}
