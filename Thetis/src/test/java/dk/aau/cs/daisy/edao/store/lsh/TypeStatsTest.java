package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TypeStatsTest
{
    private final EntityTable entTable = new EntityTable();
    private final Id id1 = Id.alloc(), id2 = Id.alloc(), id3 = Id.alloc();
    Entity ent1 = new Entity("uri1", new Type("type1"), new Type("type2"), new Type("type3")),
            ent2 = new Entity("uri2", new Type("type2")),
            ent3 = new Entity("uri3", new Type("type1"), new Type("type2"));

    @Before
    public void init()
    {
        this.entTable.insert(this.id1, this.ent1);
        this.entTable.insert(this.id2, this.ent2);
        this.entTable.insert(this.id3, this.ent3);
    }

    @Test
    public void testPercentileAll()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> all = stats.popularByPercentile(1.0);
        assertTrue(all.contains("type1"));
        assertTrue(all.contains("type2"));
        assertTrue(all.contains("type3"));
    }

    @Test
    public void testPercentileNone()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> none = stats.popularByPercentile(0.0);
        assertTrue(none.isEmpty());
    }

    @Test
    public void testPercentileSome()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> some = stats.popularByPercentile(0.6);
        assertTrue(some.contains("type1"));
        assertTrue(some.contains("type2"));
        assertFalse(some.contains("type3"));
    }
}
