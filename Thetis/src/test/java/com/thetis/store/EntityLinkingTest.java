package com.thetis.store;

import com.thetis.store.EntityLinking;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class EntityLinkingTest
{
    private final EntityLinking linker = new EntityLinking("wiki:", "uri:");

    @Before
    public void init()
    {
        this.linker.addMapping("wiki:wiki1", "uri:uri1");
        this.linker.addMapping("wiki:wiki2", "uri:uri2");
        this.linker.addMapping("wiki:wiki3", "uri:uri3");
    }

    @Test
    public void testGetDictionary()
    {
        Set<Integer> ids = new HashSet<>();
        ids.add(this.linker.inputUriLookup("wiki:wiki1").getId());
        ids.add(this.linker.inputUriLookup("wiki:wiki2").getId());
        ids.add(this.linker.inputUriLookup("wiki:wiki3").getId());
        ids.add(this.linker.kgUriLookup("uri:uri1").getId());
        ids.add(this.linker.kgUriLookup("uri:uri2").getId());
        ids.add(this.linker.kgUriLookup("uri:uri3").getId());

        assertEquals(6, ids.size());
        ids.forEach(id -> assertTrue(id >= 0));
    }

    @Test
    public void testDictionaryNotExists()
    {
        assertNull(this.linker.inputUriLookup("wiki:wiki0"));
        assertNull(this.linker.kgUriLookup("uri:uri0"));
    }

    @Test
    public void testAddDuplicates()
    {
        Set<Integer> ids1 = Set.of(this.linker.kgUriLookup("uri:uri1").getId(),
                this.linker.kgUriLookup("uri:uri2").getId(), this.linker.kgUriLookup("uri:uri3").getId());
        assertEquals(3, ids1.size());
        this.linker.addMapping("wiki:wiki1", "uri:uri1");
        this.linker.addMapping("wiki:wiki1", "uri:uri2");
        this.linker.addMapping("wiki:wiki1", "uri:uri3");

        Set<Integer> ids2 = Set.of(this.linker.kgUriLookup("uri:uri1").getId(),
                this.linker.kgUriLookup("uri:uri2").getId(), this.linker.kgUriLookup("uri:uri3").getId());
        assertEquals(3, ids2.size());
        ids2.forEach(id -> assertTrue(ids1.contains(id)));
    }

    @Test
    public void testGetWikiMapping()
    {
        assertEquals("uri:uri1", this.linker.mapTo("wiki:wiki1"));
        assertEquals("uri:uri2", this.linker.mapTo("wiki:wiki2"));
        assertEquals("uri:uri3", this.linker.mapTo("wiki:wiki3"));

        this.linker.addMapping("wiki:wiki1", "uri:uri4");
        assertEquals("uri:uri1", this.linker.mapTo("wiki:wiki1"));
    }
}
