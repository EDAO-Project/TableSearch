package dk.aau.cs.daisy.edao.loader;

import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Id;
import dk.aau.cs.daisy.edao.structures.graph.Entity;
import dk.aau.cs.daisy.edao.structures.graph.Type;
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
        List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                Path.of("table-0782-820.json"), Path.of("table-1019-555.json"), Path.of("table-1260-258.json"));
        paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
        IndexWriter writer = new IndexWriter(paths, this.outDir, new Neo4jEndpoint("config.properties"), 1,
                true, "http://www.wikipedia.org/", "http://dbpedia.org/");
        writer.performIO();
        this.reader = new IndexReader(this.outDir, false, true);
        this.reader.performIO();
    }

    @After
    public void tearDown()
    {
        this.outDir.delete();
    }

    @Test
    public void testIndexes()
    {
        EntityLinking linker = this.reader.getLinker();
        EntityTable entityTable = this.reader.getEntityTable();
        EntityTableLink entityTableLink = this.reader.getEntityTableLink();
        assertEquals(entityTable.size(), entityTableLink.size());

        int count = 0;
        Iterator<Id> iter = linker.uriIds();

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

        assertNotNull(linker.uriLookup("http://dbpedia.org/resource/1963_Formula_One_season"));
        assertNotNull(linker.wikiLookup("http://www.wikipedia.org/wiki/1963_Formula_One_season"));
        assertNotNull(linker.uriLookup("http://dbpedia.org/resource/Windows_Phone_7"));
        assertNotNull(linker.wikiLookup("http://www.wikipedia.org/wiki/Windows_Phone_7"));
    }

    @Test
    public void testEntityTable()
    {
        EntityTable entityTable = this.reader.getEntityTable();
        EntityLinking linker = this.reader.getLinker();
        Entity ent1 = entityTable.find(linker.uriLookup("http://dbpedia.org/resource/1963_Formula_One_season")),
                ent2 = entityTable.find(linker.uriLookup("http://dbpedia.org/resource/Windows_Phone_7"));
        Set<String> ent1Types = Set.of("dbo:FootballLeagueSeason", "yago:YagoPermanentlyLocatedEntity",
                "yago:Abstraction100002137", "yago:Event100029378", "yago:FundamentalQuantity113575869",
                "yago:Measure100033615", "yago:PsychologicalFeature100023100", "yago:Season115239579",
                "yago:TimePeriod115113229", "yago:WikicatFormulaOneSeasons"),
                ent2Types = Set.of("owl:Thing", "dbo:Software", "schema:CreativeWork", "dbo:Work",
                        "wikidata:Q386724", "wikidata:Q7397", "yago:Abstraction100002137",
                        "yago:Communication100033020", "yago:Explanation106738281", "yago:Interpretation107170753",
                        "yago:Message106598915", "yago:Statement106722453", "yago:Version107173585");

        // Checking URIs
        assertEquals("http://dbpedia.org/resource/1963_Formula_One_season", ent1.getUri());
        assertEquals("http://dbpedia.org/resource/Windows_Phone_7", ent2.getUri());

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

        assertEquals(1, entityTableLink.find(linking.uriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).size());
        assertEquals("table-0072-223.json", entityTableLink.find(linking.uriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).get(0));
        assertEquals(1, entityTableLink.getLocations(linking.uriLookup("http://dbpedia.org/resource/1963_Formula_One_season"), "table-0072-223.json").size());

        assertEquals(1, entityTableLink.find(linking.uriLookup("http://dbpedia.org/resource/Windows_Phone_7")).size());
        assertEquals("table-0782-820.json", entityTableLink.find(linking.uriLookup("http://dbpedia.org/resource/Windows_Phone_7")).get(0));
        assertEquals(2, entityTableLink.getLocations(linking.uriLookup("http://dbpedia.org/resource/Windows_Phone_7"), "table-0782-820.json").size());
    }
}
