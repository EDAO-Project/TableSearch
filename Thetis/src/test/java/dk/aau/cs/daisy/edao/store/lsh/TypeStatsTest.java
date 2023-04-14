package dk.aau.cs.daisy.edao.store.lsh;

import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.graph.Type;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TypeStatsTest
{
    private final EntityTable entTable = new EntityTable();
    private final EntityLinking linker = new EntityLinking("", "");
    private final String tableEnt1 = "ent1", tableEnt2 = "ent2", tableEnt3 = "ent3";
    private final Entity ent1 = new Entity("uri1", new Type("type1"), new Type("type2"), new Type("type3")),
            ent2 = new Entity("uri2", new Type("type2")),
            ent3 = new Entity("uri3", new Type("type1"), new Type("type2"));

    private final Set<Table<String>> corpus = new HashSet<>();

    @Before
    public void init()
    {
        this.linker.addMapping(this.tableEnt1, this.ent1.getUri());
        this.linker.addMapping(this.tableEnt2, this.ent2.getUri());
        this.linker.addMapping(this.tableEnt3, this.ent3.getUri());

        this.entTable.insert(this.linker.kgUriLookup(this.ent1.getUri()), this.ent1);
        this.entTable.insert(this.linker.kgUriLookup(this.ent2.getUri()), this.ent2);
        this.entTable.insert(this.linker.kgUriLookup(this.ent3.getUri()), this.ent3);

        Table<String> t1 = new DynamicTable<>(List.of(List.of(this.ent1.getUri(), this.ent2.getUri()),
                List.of(this.ent2.getUri(), this.ent3.getUri()))),
                t2 = new DynamicTable<>(List.of(List.of(this.ent1.getUri()))),
                t3 = new DynamicTable<>(List.of(List.of(this.ent2.getUri(), this.ent3.getUri())));
        this.corpus.addAll(Set.of(t1, t2, t3));
    }

    @Test
    public void testPercentileAll()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> all = stats.popularByPercentile(0.0);
        assertTrue(all.contains("type1"));
        assertTrue(all.contains("type2"));
        assertTrue(all.contains("type3"));
    }

    @Test
    public void testPercentileNone()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> none = stats.popularByPercentile(1.0);
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

    @Test
    public void testTablePercentageAll()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> types = stats.popularByTable(0.0, this.corpus, this.linker);
        assertTrue(types.contains("type1"));
        assertTrue(types.contains("type2"));
        assertTrue(types.contains("type3"));
    }

    @Test
    public void testTablePercentageSome()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> types = stats.popularByTable(0.8, this.corpus, this.linker);
        assertTrue(types.contains("type1"));
        assertTrue(types.contains("type2"));
        assertFalse(types.contains("type3"));
    }

    @Test
    public void testTablePercentageNone()
    {
        TypeStats stats = new TypeStats(this.entTable);
        Set<String> types = stats.popularByTable(1.0, this.corpus, this.linker);
        assertTrue(types.contains("type1"));
        assertTrue(types.contains("type2"));
        assertFalse(types.contains("type3"));
    }
}
