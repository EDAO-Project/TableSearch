package com.thetis.loader;

import com.thetis.TestUtils;
import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.system.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class IndexReaderTest
{
    private IndexReader reader;
    private final File outDir = new File("testing/output");

    @Before
    public void setup() throws IOException
    {
        synchronized (TestUtils.lock)
        {
            Configuration.reloadConfiguration();
            DBDriverBatch<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
            Neo4jEndpoint endpoint = new Neo4jEndpoint("config.properties");
            List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                    Path.of("table-0782-820.json"), Path.of("table-1019-555.json"), Path.of("table-1260-258.json"));
            paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
            IndexWriter writer = new IndexWriter(paths, this.outDir, new WikiLinker(endpoint), endpoint, 1,
                    embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
            writer.performIO();
            this.reader = new IndexReader(this.outDir, false, true);
            this.reader.performIO();
        }
    }

    @After
    public void tearDown()
    {
        synchronized (TestUtils.lock)
        {
            this.outDir.delete();
        }
    }

    @Test
    public void testIndexes()
    {
        EntityLinking linker = this.reader.getLinker();
        EntityTable entityTable = this.reader.getEntityTable();
        EntityTableLink entityTableLink = this.reader.getEntityTableLink();
        assertEquals(entityTable.size(), entityTableLink.size());

        int count = 0;
        Iterator<Id> iter = linker.kgUriIds();

        while (iter.hasNext())
        {
            count++;
            iter.next();
        }

        assertEquals(entityTable.size(), count);
    }

    @Test
    public void testLinker()
    {
        EntityLinking linker = this.reader.getLinker();
        assertEquals("http://dbpedia.org/resource/1963_Formula_One_season", linker.mapTo("http://www.wikipedia.org/wiki/1963_Formula_One_season"));
        assertEquals("http://www.wikipedia.org/wiki/1963_Formula_One_season", linker.mapFrom("http://dbpedia.org/resource/1963_Formula_One_season"));
        assertEquals("http://dbpedia.org/resource/Windows_Phone_7", linker.mapTo("http://www.wikipedia.org/wiki/Windows_Phone_7"));
        assertEquals("http://www.wikipedia.org/wiki/Windows_Phone_7", linker.mapFrom("http://dbpedia.org/resource/Windows_Phone_7"));

        assertNotNull(linker.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season"));
        assertNotNull(linker.inputUriLookup("http://www.wikipedia.org/wiki/1963_Formula_One_season"));
        assertNotNull(linker.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7"));
        assertNotNull(linker.inputUriLookup("http://www.wikipedia.org/wiki/Windows_Phone_7"));
    }

    @Test
    public void testEntityTable()
    {
        EntityTable entityTable = this.reader.getEntityTable();
        EntityLinking linker = this.reader.getLinker();
        Entity ent1 = entityTable.find(linker.kgUriLookup("http://dbpedia.org/resource/Boston_Bruins")),
                ent2 = entityTable.find(linker.kgUriLookup("http://dbpedia.org/resource/NEC_Cup"));
        Set<String> ent1Types = Set.of("http://dbpedia.org/ontology/HockeyTeam", "http://dbpedia.org/ontology/Agent",
                "http://dbpedia.org/ontology/Organisation", "http://dbpedia.org/ontology/SportsTeam",
                "http://schema.org/Organization", "http://schema.org/SportsTeam", "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#Agent",
                "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#SocialPerson",
                "http://www.wikidata.org/entity/Q12973014", "http://www.wikidata.org/entity/Q24229398",
                "http://www.wikidata.org/entity/Q43229", "http://www.wikidata.org/entity/Q4498974"),
                ent2Types = Set.of();

        // Checking URIs
        assertEquals("http://dbpedia.org/resource/Boston_Bruins", ent1.getUri());
        assertEquals("http://dbpedia.org/resource/NEC_Cup", ent2.getUri());

        // Checking types
        assertEquals(ent1Types.size(), ent1.getTypes().size());
        assertEquals(ent2Types.size(), ent2.getTypes().size());

        for (Type t : ent1.getTypes())
        {
            assertTrue(ent1Types.contains(t.getType()));
            assertTrue(t.getIdf() > 0);
        }

        for (Type t : ent2.getTypes())
        {
            assertTrue(ent2Types.contains(t.getType()));
            assertTrue(t.getIdf() > 0);
        }
    }

    @Test
    public void testEntityTableLink()
    {
        EntityTableLink entityTableLink = this.reader.getEntityTableLink();
        EntityLinking linking = this.reader.getLinker();

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).size());
        assertEquals("table-0072-223.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).get(0));
        assertEquals(1, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season"), "table-0072-223.json").size());

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).size());
        assertEquals("table-0782-820.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).get(0));
        assertEquals(2, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7"), "table-0782-820.json").size());
    }
}
