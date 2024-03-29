package com.thetis.search;

import com.thetis.TestUtils;
import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Factory;
import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.IndexWriter;
import com.thetis.loader.WikiLinker;
import com.thetis.structures.Pair;
import com.thetis.structures.table.SimpleTable;
import com.thetis.structures.table.Table;
import com.thetis.system.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnalogousSearchTest
{
    private AnalogousSearch search;
    private final File outDir = new File("testing/output");

    @Before
    public void setup() throws IOException
    {
        synchronized (TestUtils.lock)
        {
            Configuration.reloadConfiguration();
            Neo4jEndpoint endpoint = new Neo4jEndpoint("config.properties");
            DBDriverBatch<List<Double>, String> embeddingsDB = Factory.fromConfig(false);
            List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                    Path.of("table-0782-820.json"), Path.of("table-1019-555.json"),
                    Path.of("table-1260-258.json"), Path.of("table-0001-1.json"));
            paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
            IndexWriter indexWriter = new IndexWriter(paths, this.outDir, new WikiLinker(endpoint), endpoint,
                    1, embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
            indexWriter.performIO();

            this.search = new AnalogousSearch(indexWriter.getEntityLinker(), indexWriter.getEntityTable(), indexWriter.getEntityTableLinker(),
                    indexWriter.getEmbeddingsIndex(), 5, 1, AnalogousSearch.EntitySimilarity.JACCARD_TYPES,
                    false, false, true, false,
                    false, AnalogousSearch.SimilarityMeasure.EUCLIDEAN);
        }
    }

    @After
    public void tearDown()
    {
        this.outDir.delete();
    }

    @Test
    public void testTableScore()
    {
        Table<String> query = new SimpleTable<>(List.of(List.of("http://dbpedia.org/resource/Windows_Mobile", "http://dbpedia.org/resource/Cosworth")));
        Result result = this.search.search(query);
        assertTrue(result.getResults().hasNext());

        Pair<String, Double> best = result.getResults().next();
        assertEquals("table-0001-1.json", best.getFirst());
        assertEquals(0.739, best.getSecond(), 0.001);
    }

    @Test
    public void testOneTupleQuery()
    {
        Table<String> query = new SimpleTable<>(List.of(List.of("http://dbpedia.org/resource/1963_Formula_One_season",
                "http://dbpedia.org/resource/Team_Lotus")));
        Result result = this.search.search(query);
        assertEquals(5, result.getK());
        assertEquals(this.search.getParsedTables(), result.getSize());

        List<Pair<String, Double>> resultList = new ArrayList<>(result.getSize());
        Iterator<Pair<String, Double>> resultIter = result.getResults();

        while (resultIter.hasNext())
        {
            resultList.add(resultIter.next());
        }

        assertEquals(5, resultList.size());
        assertEquals("table-0072-223.json", resultList.get(0).getFirst());
    }

    @Test
    public void testTwoTupleQuery()
    {
        Table<String> query = new SimpleTable<>(List.of(List.of("http://dbpedia.org/resource/1971_Formula_One_season",
                "http://dbpedia.org/resource/North_American_Racing_Team"),
                List.of("http://dbpedia.org/resource/St._Louis_Blues", "http://dbpedia.org/resource/California_Golden_Seals")));
        Result result = this.search.search(query);
        assertEquals(5, result.getK());
        assertEquals(this.search.getParsedTables(), result.getSize());

        List<Pair<String, Double>> resultList = new ArrayList<>(result.getSize());
        Iterator<Pair<String, Double>> resultIter = result.getResults();

        while (resultIter.hasNext())
        {
            resultList.add(resultIter.next());
        }

        assertEquals(5, resultList.size());
        assertEquals("table-0072-223.json", resultList.get(0).getFirst());
    }

    @Test
    public void testThreeTupleQuery()
    {
        Table<String> query = new SimpleTable<>(List.of(List.of("http://dbpedia.org/resource/Windows_Mobile"),
                List.of("http://dbpedia.org/resource/Maemo"), List.of("http://dbpedia.org/resource/BlackBerry_10")));
        Result result = this.search.search(query);
        assertEquals(5, result.getK());
        assertEquals(this.search.getParsedTables(), result.getSize());

        List<Pair<String, Double>> resultList = new ArrayList<>(result.getSize());
        Iterator<Pair<String, Double>> resultIter = result.getResults();

        while (resultIter.hasNext())
        {
            resultList.add(resultIter.next());
        }

        assertEquals(5, resultList.size());
        assertEquals("table-0782-820.json", resultList.get(0).getFirst());
        assertEquals(1.0, resultList.get(0).getSecond(), 0.01);    // Score should be 0.923
    }
}
