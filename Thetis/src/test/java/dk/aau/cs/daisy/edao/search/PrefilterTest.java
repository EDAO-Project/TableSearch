package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.loader.IndexWriter;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Pair;
import dk.aau.cs.daisy.edao.structures.PairNonComparable;
import dk.aau.cs.daisy.edao.structures.table.DynamicTable;
import dk.aau.cs.daisy.edao.structures.table.Table;
import dk.aau.cs.daisy.edao.system.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PrefilterTest
{
    private final File outDir = new File("testing/output");
    private Prefilter typesPrefilter;
    private Prefilter embeddingsPrefilter;
    private PairNonComparable<Table<String>, String> singleQuery, nQuery;

    @Before
    public void setup() throws IOException
    {
        Configuration.reloadConfiguration();
        List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                Path.of("table-0782-820.json"), Path.of("table-1019-555.json"),
                Path.of("table-1260-258.json"), Path.of("table-0001-1.json"), Path.of("table-0001-2.json"));
        paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
        IndexWriter indexWriter = new IndexWriter(paths, this.outDir, new Neo4jEndpoint("config.properties"), 1,
                true, "http://www.wikipedia.org/", "http://dbpedia.org/");
        indexWriter.performIO();

        EntityLinking linker = indexWriter.getEntityLinker();
        EntityTable entityTable = indexWriter.getEntityTable();
        EntityTableLink tableLink = indexWriter.getEntityTableLinker();
        this.typesPrefilter = new Prefilter(linker, entityTable, tableLink, indexWriter.getTypesLSH());
        this.embeddingsPrefilter = new Prefilter(linker, entityTable, tableLink, indexWriter.getEmbeddingsLSH());

        String singleUri = linker.mapTo("http://www.wikipedia.org/wiki/WebOS");
        this.singleQuery = new PairNonComparable<>(new DynamicTable<>(List.of(List.of(singleUri))), "table-0001-2.json");

        String uri1 = linker.mapTo("http://www.wikipedia.org/wiki/WebOS"),
                uri2 = linker.mapTo("http://www.wikipedia.org/wiki/Boston_Bruins"),
                uri3 = linker.mapTo("http://www.wikipedia.org/wiki/Maemo"),
                uri4 = linker.mapTo("http://www.wikipedia.org/wiki/Chicago_Blackhawks");
        this.nQuery = new PairNonComparable<>(new DynamicTable<>(List.of(List.of(uri1, uri2), List.of(uri3, uri4))),
                "table-0001-1.json");
    }

    @Test
    public void testOneEntityTableTypesLSH()
    {
        Iterator<Pair<String, Double>> results = this.typesPrefilter.search(this.singleQuery.getFirst()).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            Pair<String, Double> result = results.next();

            if (result.getFirst().equals(this.singleQuery.getSecond()))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testOneEntityTableEmbeddingsLSH()
    {
        Iterator<Pair<String, Double>> results = this.embeddingsPrefilter.search(this.singleQuery.getFirst()).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            Pair<String, Double> result = results.next();

            if (result.getFirst().equals(this.singleQuery.getSecond()))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testNEntityTableWithRemovalTypesLSH()
    {
        List<List<String>> queryMatrix = new ArrayList<>();

        for (int i = 0; i < this.nQuery.getFirst().rowCount(); i++)
        {
            queryMatrix.add(new ArrayList<>(this.nQuery.getFirst().getRow(i).size()));

            for (int j = 0; j < this.nQuery.getFirst().getRow(i).size(); j++)
            {
                queryMatrix.get(i).add(this.nQuery.getFirst().getRow(i).get(j));
            }
        }

        Table<String> query = new DynamicTable<>(queryMatrix);
        Iterator<Pair<String, Double>> results = this.typesPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            if (results.next().getFirst().equals(this.nQuery.getSecond()))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testNEntityTableWithRemovalEmbeddingsLSH()
    {
        List<List<String>> queryMatrix = new ArrayList<>();

        for (int i = 0; i < this.nQuery.getFirst().rowCount(); i++)
        {
            queryMatrix.add(new ArrayList<>(this.nQuery.getFirst().getRow(i).size()));

            for (int j = 0; j < this.nQuery.getFirst().getRow(i).size(); j++)
            {
                queryMatrix.get(i).add(this.nQuery.getFirst().getRow(i).get(j));
            }
        }

        Table<String> query = new DynamicTable<>(queryMatrix);
        Iterator<Pair<String, Double>> results = this.embeddingsPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            if (results.next().getFirst().equals(this.nQuery.getSecond()))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }
}
