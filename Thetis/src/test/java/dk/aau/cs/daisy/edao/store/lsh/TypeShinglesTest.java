package dk.aau.cs.daisy.edao.store.lsh;

import com.thetis.store.lsh.ElementShingles;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class TypeShinglesTest
{
    @Test
    public void test1()
    {
        Set<String> types = Set.of("t1", "t2", "t3");
        Set<List<String>> shingles = ElementShingles.shingles(types, 2);
        assertTrue(shingles.contains(List.of("t1", "t2")));
        assertTrue(shingles.contains(List.of("t1", "t3")));
        assertTrue(shingles.contains(List.of("t2", "t1")));
        assertTrue(shingles.contains(List.of("t2", "t3")));
        assertTrue(shingles.contains(List.of("t3", "t1")));
        assertTrue(shingles.contains(List.of("t3", "t2")));
    }
}
