package com.thetis.store;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.structures.Id;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class EmbeddingsIndexTest
{
    private EmbeddingsIndex<String> idx = new EmbeddingsIndex<>();

    @Before
    public void load()
    {
        for (int i = 0; i < 10; i++)
        {
            Id id = new Id(i);
            List<Double> embeddings = new ArrayList<>(Collections.nCopies(10, (double) i));
            this.idx.insert(id, embeddings);
        }

        this.idx.clusterInsert("cluster1", new Id(1), List.of(1.0, 2.0, 3.0));
        this.idx.clusterInsert("cluster1", new Id(2), List.of(4.0, 5.0, 6.0));
        this.idx.clusterInsert("cluster1", new Id(3), List.of(7.0, 8.0, 9.0));
        this.idx.clusterInsert("cluster2", new Id(4), List.of(1.0, 1.0, 1.0));
        this.idx.clusterInsert("cluster2", new Id(5), List.of(2.0, 2.0, 2.0));
        this.idx.clusterInsert("cluster2", new Id(6), List.of(3.0, 3.0, 3.0));
        this.idx.clusterInsert("cluster3", new Id(7), List.of(9.0, 8.0, 7.0));
        this.idx.clusterInsert("cluster3", new Id(8), List.of(6.0, 5.0, 4.0));
        this.idx.clusterInsert("cluster3", new Id(9), List.of(3.0, 2.0, 1.0));
    }

    @Test
    public void testSize()
    {
        assertEquals(10, this.idx.size());
        assertEquals(3, this.idx.clusters());
    }

    @Test
    public void testRemove()
    {
        assertTrue(this.idx.remove(new Id(0)));
        assertEquals(9, this.idx.size());

        assertTrue(this.idx.remove(new Id(4)));
        assertEquals(8, this.idx.size());

        assertFalse(this.idx.remove(new Id(100)));
        assertEquals(8, this.idx.size());
    }

    @Test
    public void testFind()
    {
        List<Double> expected = new ArrayList<>(Collections.nCopies(10, 0.0));
        assertEquals(expected, this.idx.find(new Id(0)));

        expected = new ArrayList<>(Collections.nCopies(10, 4.0));
        assertEquals(expected, this.idx.find(new Id(4)));
    }

    @Test
    public void testContains()
    {
        assertTrue(this.idx.contains(new Id(2)));
        assertTrue(this.idx.contains(new Id(7)));
        assertTrue(this.idx.contains(new Id(9)));

        assertFalse(this.idx.contains(new Id(10)));
        assertFalse(this.idx.contains(new Id(100)));
        assertFalse(this.idx.contains(new Id(-1)));
    }

    @Test
    public void testClear()
    {
        assertEquals(10, this.idx.size());

        this.idx.clear();
        assertEquals(0, this.idx.size());
    }

    @Test
    public void testClusterRemove()
    {
        assertFalse(this.idx.clusterRemove("cluster5"));
        assertEquals(3, this.idx.clusters());

        assertTrue(this.idx.clusterRemove("cluster2"));
        assertEquals(2, this.idx.clusters());

        assertTrue(this.idx.clusterRemove("cluster1"));
        assertEquals(1, this.idx.clusters());
    }

    @Test
    public void testContainsCluster()
    {
        assertTrue(this.idx.containsCluster("cluster1"));
        assertTrue(this.idx.containsCluster("cluster2"));
        assertTrue(this.idx.containsCluster("cluster3"));
        assertFalse(this.idx.containsCluster("cluster4"));
        assertFalse(this.idx.containsCluster("cluster5"));
    }

    @Test
    public void testDropClusters()
    {
        assertEquals(3, this.idx.clusters());
        this.idx.dropClusters();
        assertEquals(0, this.idx.clusters());
    }

    @Test
    public void testClusterGet()
    {
        List<Double> expected = List.of(4.0, 5.0, 6.0);
        assertEquals(expected, this.idx.clusterGet("cluster1", new Id(2)));

        expected = List.of(3.0, 2.0, 1.0);
        assertEquals(expected, this.idx.clusterGet("cluster3", new Id(9)));
    }
}