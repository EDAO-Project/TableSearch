package com.thetis.connector;

import com.thetis.connector.SQLite;
import com.thetis.connector.embeddings.RelationalEmbeddings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RelationalEmbeddingsTest
{
    private RelationalEmbeddings embeddings;

    @Before
    public void setup()
    {
        this.embeddings = new RelationalEmbeddings(SQLite.init("test.db"));
        this.embeddings.setup();

        this.embeddings.update("iriA 1,1,1");
        this.embeddings.update("iriB 2,2,2");
        this.embeddings.update("iriC 3,3,3");
    }

    @After
    public void tearDown()
    {
        File f = new File("test.db");
        this.embeddings.close();

        if (f.exists())
            f.delete();
    }

    @Test
    public void testInsert()
    {
        assertTrue(this.embeddings.update("iri1 1,2,3"));
        assertTrue(this.embeddings.update("iri2 4,5,6"));
        assertTrue(this.embeddings.update("iri3 7,8,9"));
    }

    @Test
    public void testBatchInsert()
    {
        List<String> iris = List.of("iri1", "iri2", "iri3");
        List<List<Float>> vectors = List.of(List.of(1.0f, 2.0f, 3.0f), List.of(4.0f, 5.0f, 6.0f), List.of(7.0f, 8.0f, 9.0f));
        assertTrue(this.embeddings.batchInsert(iris, vectors));
    }

    @Test
    public void testSelect()
    {
        assertEquals(List.of(1.0, 1.0, 1.0), this.embeddings.select("iriA"));
        assertEquals(List.of(2.0, 2.0, 2.0), this.embeddings.select("iriB"));
        assertEquals(List.of(3.0, 3.0, 3.0), this.embeddings.select("iriC"));
    }
}
