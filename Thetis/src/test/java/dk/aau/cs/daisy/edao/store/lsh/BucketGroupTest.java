package dk.aau.cs.daisy.edao.store.lsh;

import com.thetis.store.lsh.BucketGroup;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BucketGroupTest
{
    private static BucketGroup<String, String> create()
    {
        BucketGroup<String, String> group = new BucketGroup<>(3);
        group.add(0, "1", "a");
        group.add(0, "2", "b");
        group.add(0, "3", "c");
        group.add(1, "4", "d");
        group.add(1, "5", "d");
        group.add(1, "6", "d");
        group.add(2, "7", "e");
        group.add(2, "7", "f");
        group.add(2, "7", "g");

        return group;
    }

    @Test
    public void testGetBuckets()
    {
        BucketGroup<String, String> group = create();
        assertEquals(3, group.buckets().size());
        assertEquals(3, group.size());
    }

    @Test
    public void testGet()
    {
        BucketGroup<String, String> group = create();
        Set<String> b1Values = Set.of("a", "b", "c"), b2Values = Set.of("d"), b3Values = Set.of("e", "f", "g");
        Set<String> bucket1 = group.get(0), bucket2 = group.get(1), bucket3 = group.get(2);
        assertEquals(b1Values.size(), bucket1.size());
        assertEquals(b2Values.size(), bucket2.size());
        assertEquals(b3Values.size(), bucket3.size());

        for (String val : bucket1)
        {
            assertTrue(b1Values.contains(val));
        }

        for (String val : bucket2)
        {
            assertTrue(b2Values.contains(val));
        }

        for (String val : bucket3)
        {
            assertTrue(b3Values.contains(val));
        }
    }
}
