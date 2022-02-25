package dk.aau.cs.daisy.edao.connector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EmbeddingStoreTest
{
    private EmbeddingStore store;

    @Before
    public void load()
    {
        this.store = new EmbeddingStore("./", "localhost", 19530, 3);
        this.store.update("iri1 1,2,3");
        this.store.update("iri2 4,5,6");
        this.store.update("iri3 7,8,9");
    }

    @After
    public void clean()
    {
        File f = new File("embeddings.db");

        if (f.exists())
            f.delete();

        this.store.drop(null);
        this.store.close();
    }

    @Test
    public void testRead()
    {
        List<Double> e1 = this.store.select("iri1"),
                e2 = this.store.select("iri2"),
                e3 = this.store.select("iri3");

        assertNotNull(e1);
        assertNotNull(e2);
        assertNotNull(e3);
        assertEquals(3, e1.size());
        assertEquals(3, e2.size());
        assertEquals(3, e3.size());
    }
}
