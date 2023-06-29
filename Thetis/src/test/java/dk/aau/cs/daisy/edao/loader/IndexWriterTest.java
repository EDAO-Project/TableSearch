package dk.aau.cs.daisy.edao.loader;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.IndexWriter;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.system.Configuration;
import dk.aau.cs.daisy.edao.TestUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class IndexWriterTest
{
    private IndexWriter writer;
    private final File outDir = new File("testing/output");

    @Before
    public void setup() throws IOException
    {
        synchronized (TestUtils.lock)
        {
            Configuration.reloadConfiguration();
            DBDriverBatch<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
            List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                    Path.of("table-0782-820.json"), Path.of("table-1019-555.json"), Path.of("table-1260-258.json"));
            paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
            this.writer = new IndexWriter(paths, this.outDir, new Neo4jEndpoint("config.properties"), 1,
                    true, embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
            this.writer.performIO();
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
        EntityLinking linker = this.writer.getEntityLinker();
        EntityTable entityTable = this.writer.getEntityTable();
        EntityTableLink entityTableLink = this.writer.getEntityTableLinker();
        assertEquals(135, entityTable.size());
        assertEquals(entityTable.size(), entityTableLink.size());

        int count = 0;
        Iterator<Id> iter = linker.kgUriIds();

        for (; iter.hasNext(); count++)
        {
            iter.next();
        }

        assertEquals(135, count);
    }

    @Test
    public void testStats()
    {
        assertEquals(5, this.writer.loadedTables());
        assertEquals(197, this.writer.cellsWithLinks());
    }

    @Test
    public void testLinker()
    {
        EntityLinking linker = this.writer.getEntityLinker();
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
        EntityTable entityTable = this.writer.getEntityTable();
        EntityLinking linker = this.writer.getEntityLinker();
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

        // Checking entity IDFs
        assertEquals(Math.log10(this.writer.loadedTables()) + 1, ent1.getIDF(), 0.0001);
        assertEquals(Math.log10(this.writer.loadedTables()) + 1, ent2.getIDF(), 0.0001);

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
        EntityTableLink entityTableLink = this.writer.getEntityTableLinker();
        EntityLinking linking = this.writer.getEntityLinker();

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).size());
        assertEquals("table-0072-223.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).get(0));
        assertEquals(1, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season"), "table-0072-223.json").size());

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).size());
        assertEquals("table-0782-820.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).get(0));
        assertEquals(2, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7"), "table-0782-820.json").size());
    }
}
