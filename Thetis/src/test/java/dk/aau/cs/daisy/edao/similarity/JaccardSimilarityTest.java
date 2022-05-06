package dk.aau.cs.daisy.edao.similarity;

import dk.aau.cs.daisy.edao.structures.graph.Type;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class JaccardSimilarityTest
{
    private Set<Type> ts1 = Set.of(new Type("t1", 231.23), new Type("t2", 2.11),
            new Type("t3", 534.21), new Type("t4", 5.64), new Type("t5", 31.74)),
            ts2 = Set.of(new Type("t1"), new Type("t2"), new Type("t3"), new Type("t4"), new Type("t5")),
            ts3 = Set.of(new Type("t2"), new Type("t3", 534.21), new Type("t4", 5.64)),
            ts4 = Set.of(new Type("t6", 3.22), new Type("t7", 2.13), new Type("t8", 9.21),
                    new Type("t9", 8.44), new Type("t10", 32.21));

    @Test
    public void testFullIntersection()
    {
        assertEquals(1.0, JaccardSimilarity.make(this.ts1, this.ts1).similarity(), 0.0);
    }

    @Test
    public void testEmptyIntersection()
    {
        assertEquals(0.0, JaccardSimilarity.make(this.ts1, this.ts2).similarity(), 0.0);
        assertEquals(0.0, JaccardSimilarity.make(this.ts1, this.ts4).similarity(), 0.0);
    }

    @Test
    public void testPartialIntersection()
    {
        assertEquals((double) 1 / 3, JaccardSimilarity.make(this.ts1, ts3).similarity(), 0.0001);
        assertEquals((double) 1 / 7, JaccardSimilarity.make(this.ts2, this.ts3).similarity(), 0.0001);
    }
}
