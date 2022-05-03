package dk.aau.cs.daisy.edao.store;

import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class EntityTableLinkTest
{
    private final EntityTableLink tableLink = new EntityTableLink();
    private Id id1 = Id.alloc(), id2 = Id.alloc(), id3 = Id.alloc();
    private final List<String> files1 = List.of("file1", "file2", "file3"),
            files2 = List.of("file1", "file2"),
            files3 = List.of("file2", "file3");

    @Before
    public void init()
    {
        this.tableLink.insert(this.id1, this.files1);
        this.tableLink.insert(this.id2, this.files2);
        this.tableLink.insert(this.id3, this.files3);
    }

    @Test
    public void testContains()
    {
        assertTrue(this.tableLink.contains(this.id1));
        assertTrue(this.tableLink.contains(this.id2));
        assertTrue(this.tableLink.contains(this.id3));
        assertFalse(this.tableLink.contains(Id.alloc()));
    }

    @Test
    public void testRemove()
    {
        assertTrue(this.tableLink.remove(this.id2));
        assertFalse(this.tableLink.contains(this.id2));
    }

    @Test
    public void testFind()
    {
        List<String> fileNames1 = this.tableLink.find(this.id1),
                fileNames2 = this.tableLink.find(this.id2),
                fileNames3 = this.tableLink.find(this.id3);

        assertEquals(this.files1.size(), fileNames1.size());
        assertEquals(this.files2.size(), fileNames2.size());
        assertEquals(this.files3.size(), fileNames3.size());
        fileNames1.forEach(f -> assertTrue(this.files1.contains(f)));
        fileNames2.forEach(f -> assertTrue(this.files2.contains(f)));
        fileNames3.forEach(f -> assertTrue(this.files3.contains(f)));
    }

    @Test
    public void testGetLocations()
    {
        List<Pair<Integer, Integer>> loc1 = List.of(new Pair<>(0, 0), new Pair<>(1, 1)),
                loc2 = List.of(new Pair<>(2, 2), new Pair<>(3, 3)),
                loc3 = List.of(new Pair<>(4, 4), new Pair<>(5, 5));
        this.tableLink.addLocation(this.id1, this.files1.get(0), loc1);
        this.tableLink.addLocation(this.id1, this.files1.get(1), loc2);

        Id id4 = Id.alloc();
        this.tableLink.addLocation(id4, "file4", loc3);
        assertEquals(List.of("file4"), this.tableLink.find(id4));

        List<Pair<Integer, Integer>> locations1 = this.tableLink.getLocations(this.id1, this.files1.get(0)),
                locations2 = this.tableLink.getLocations(this.id1, this.files1.get(1)),
                locations3 = this.tableLink.getLocations(id4, "file4");
        assertEquals(loc1, locations1);
        assertEquals(loc2, locations2);
        assertEquals(loc3, locations3);
    }

    @Test
    public void testTableToEntities()
    {
        Set<Id> entities1 = this.tableLink.tableToEntities("file1"),
                entities2 = this.tableLink.tableToEntities("file2"),
                entities3 = this.tableLink.tableToEntities("file3");
        assertEquals(2, entities1.size());
        assertEquals(3, entities2.size());
        assertEquals(2, entities3.size());
        assertTrue(entities1.contains(this.id1) && entities1.contains(this.id2));
        assertTrue(entities2.contains(this.id1) && entities2.contains(this.id2) && entities2.contains(this.id3));
        assertTrue(entities3.contains(this.id1) && entities3.contains(this.id3));
    }

    @Test
    public void testSerialization()
    {
        File indexFile = new File("index.idx");

        try (ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(indexFile)))
        {
            stream.writeObject(this.tableLink);
        }

        catch (IOException e)
        {
            indexFile.delete();
            fail(e.getMessage());
        }

        try (ObjectInputStream stream = new ObjectInputStream(new FileInputStream(indexFile)))
        {
            EntityTableLink index = (EntityTableLink) stream.readObject();
            indexFile.delete();
            assertEquals(3, index.size());

            List<String> id1Files = index.find(this.id1), id2Files = index.find(this.id2), id3Files = index.find(this.id3);
            this.files1.forEach(f -> assertTrue(id1Files.contains(f)));
            this.files2.forEach(f -> assertTrue(id2Files.contains(f)));
            this.files3.forEach(f -> assertTrue(id3Files.contains(f)));
        }

        catch (IOException | ClassNotFoundException e)
        {
            indexFile.delete();
            fail(e.getMessage());
        }
    }
}
