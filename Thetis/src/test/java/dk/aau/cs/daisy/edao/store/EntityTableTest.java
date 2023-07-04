package dk.aau.cs.daisy.edao.store;

import com.thetis.store.EntityTable;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class EntityTableTest
{
    private final EntityTable entTable = new EntityTable();
    private final Id id1 = Id.alloc(), id2 = Id.alloc(), id3 = Id.alloc();
    Entity ent1 = new Entity("uri1", List.of(new Type("type1"), new Type("type2"), new Type("type3")), List.of()),
            ent2 = new Entity("uri2", List.of(new Type("type2"), new Type("type3")), List.of()),
            ent3 = new Entity("uri3", List.of(new Type("type1"), new Type("type2")), List.of());

    @Before
    public void init()
    {
        this.entTable.insert(this.id1, this.ent1);
        this.entTable.insert(this.id2, this.ent2);
        this.entTable.insert(this.id3, this.ent3);
    }

    @Test
    public void testContains()
    {
        assertTrue(this.entTable.contains(this.id1));
        assertTrue(this.entTable.contains(this.id2));
        assertTrue(this.entTable.contains(this.id3));
        assertFalse(this.entTable.contains(Id.alloc()));
    }

    @Test
    public void testRemove()
    {
        assertTrue(this.entTable.remove(this.id1));
        assertTrue(this.entTable.remove(this.id3));
        assertTrue(this.entTable.contains(this.id2));
        assertFalse(this.entTable.contains(this.id1));
        assertFalse(this.entTable.contains(this.id3));
    }

    @Test
    public void testFind()
    {
        assertEquals(this.ent1, this.entTable.find(this.id1));
        assertEquals(this.ent2, this.entTable.find(this.id2));
        assertEquals(this.ent3, this.entTable.find(this.id3));
    }
}
