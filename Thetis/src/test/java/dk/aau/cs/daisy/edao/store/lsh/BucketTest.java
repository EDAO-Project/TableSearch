package dk.aau.cs.daisy.edao.store.lsh;

import com.thetis.store.lsh.Bucket;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BucketTest
{
    private static Bucket<String, String> create()
    {
        Bucket<String, String> bucket = new Bucket<>();
        bucket.add("1", "a");
        bucket.add("2", "b");
        bucket.add("3", "c");
        bucket.add("4", "d");
        bucket.add("5", "d");
        bucket.add("6", "d");
        bucket.add("7", "e");
        bucket.add("7", "f");
        bucket.add("7", "g");

        return bucket;
    }

    @Test
    public void testFind()
    {
        Bucket<String, String> bucket = create();

        assertEquals(Set.of("a"), bucket.find("1"));
        assertEquals(Set.of("b"), bucket.find("2"));
        assertEquals(Set.of("c"), bucket.find("3"));

        assertEquals(Set.of("d"), bucket.find("4"));
        assertEquals(Set.of("d"), bucket.find("5"));
        assertEquals(Set.of("d"), bucket.find("6"));
        assertEquals(Set.of("e", "f", "g"), bucket.find("7"));
    }

    @Test
    public void testAll()
    {
        Bucket<String, String> bucket = create();
        assertEquals(Set.of("a", "b", "c", "d", "e", "f", "g"), bucket.all());
    }

    @Test
    public void testDelete()
    {
        Bucket<String, String> bucket = create();
        assertTrue(bucket.delete("7"));
        assertEquals(Set.of("a", "b", "c", "d"), bucket.all());
    }
}
