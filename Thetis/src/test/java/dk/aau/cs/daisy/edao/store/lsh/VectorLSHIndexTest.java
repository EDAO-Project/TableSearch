package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.structures.PairNonComparable;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorLSHIndexTest
{
    private static VectorLSHIndex create()
    {
        Set<PairNonComparable<String, Set<PairNonComparable<String, List<Double>>>>> tableVectors = new HashSet<>();
        HashFunction hash = (obj, num) -> {
            List<Boolean> bits = (List<Boolean>) obj;
            int sum = 0;

            for (int i = 0; i < bits.size(); i++)
            {
                sum += Math.pow(bits.get(i) ? 2 : 0, i);
            }

            return sum % num;
        };

        String table1 = "t1";
        Set<PairNonComparable<String, List<Double>>> embeddings1 = new HashSet<>();
        embeddings1.add(new PairNonComparable<>("entity1", List.of(0.4, 0.1, -0.6, 0.01)));
        embeddings1.add(new PairNonComparable<>("entity2", List.of(-0.9, -0.5, 0.1, -0.1)));
        embeddings1.add(new PairNonComparable<>("entity1", List.of(0.9, 0.8, -0.3, 0.2)));
        embeddings1.add(new PairNonComparable<>("entity2", List.of(0.95, 0.2, -0.01, 0.3)));
        tableVectors.add(new PairNonComparable<>(table1, embeddings1));

        String table2 = "t2";
        Set<PairNonComparable<String, List<Double>>> embeddings2 = new HashSet<>();
        embeddings2.add(new PairNonComparable<>("entity2", List.of(0.5, -0.5, -0.2, 0.2)));
        embeddings2.add(new PairNonComparable<>("entity3", List.of(0.4, -0.4, -0.3, 0.3)));
        embeddings2.add(new PairNonComparable<>("entity4", List.of(0.5, -0.4, -0.2, 0.3)));
        embeddings2.add(new PairNonComparable<>("entity5", List.of(0.4, -0.5, -0.3, 0.3)));
        tableVectors.add(new PairNonComparable<>(table2, embeddings2));

        String table3 = "t3";
        Set<PairNonComparable<String, List<Double>>> embeddings3 = new HashSet<>();
        embeddings3.add(new PairNonComparable<>("entity1", List.of(0.4, 0.2, 0.1, -0.1)));
        embeddings3.add(new PairNonComparable<>("entity3", List.of(0.9, -0.9, 0.2, -0.5)));
        embeddings3.add(new PairNonComparable<>("entity4", List.of(-0.2, -0.4, 0.2, -0.7)));
        embeddings3.add(new PairNonComparable<>("entity5", List.of(0.6, -0.2, 0.4, 0.3)));
        tableVectors.add(new PairNonComparable<>(table3, embeddings3));

        return new VectorLSHIndex(3, 2, hash, tableVectors, 4);
    }

    @Test
    public void testBucketCount()
    {
        VectorLSHIndex index = create();
        assertEquals(3, index.buckets());
    }

    @Test
    public void testSearch()
    {
        VectorLSHIndex index = create();
        PairNonComparable<String, List<Double>> query = new PairNonComparable<>("", List.of(0.5, -0.5, -0.2, 0.2));
        Set<String> tables = index.search(query);
        assertTrue(tables.contains("t2"));
    }
}
