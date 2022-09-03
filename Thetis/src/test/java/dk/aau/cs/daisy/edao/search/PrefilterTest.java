package dk.aau.cs.daisy.edao.search;

import dk.aau.cs.daisy.edao.commands.parser.TableParser;
import dk.aau.cs.daisy.edao.connector.Neo4jEndpoint;
import dk.aau.cs.daisy.edao.loader.IndexWriter;
import dk.aau.cs.daisy.edao.store.EntityLinking;
import dk.aau.cs.daisy.edao.store.EntityTable;
import dk.aau.cs.daisy.edao.store.EntityTableLink;
import dk.aau.cs.daisy.edao.structures.Pair;
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
    private static final int TOP_K = 5;

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
    }

    @Test
    public void testOneEntityTableTypesLSH()
    {
        Table<String> query = TableParser.toTable(new File("testing/data/table-0001-2.json"));
        assertNotNull("Could not parse query table", query);

        Iterator<Pair<String, Double>> results = this.typesPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            Pair<String, Double> result = results.next();

            if (result.getFirst().equals("table-0001-2.json"))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testOneEntityTableEmbeddingsLSH()
    {
        Table<String> query = TableParser.toTable(new File("testing/data/table-0001-2.json"));
        assertNotNull("Could not parse query table", query);

        Iterator<Pair<String, Double>> results = this.embeddingsPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            Pair<String, Double> result = results.next();

            if (result.getFirst().equals("table-0001-2.json"))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testNEntityTableWithRemovalTypesLSH()
    {
        Table<String> table = TableParser.toTable(new File("testing/data/table-0001-1.json"));
        assertNotNull(table);

        Iterator<String> rowElements = table.getRow(0).iterator();
        List<String> row = new ArrayList<>();

        while (rowElements.hasNext())
        {
            row.add(rowElements.next());
        }

        Table<String> query = new DynamicTable<>(List.of(row));
        Iterator<Pair<String, Double>> results = this.typesPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            if (results.next().getFirst().equals("table-0001-1.json"))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }

    @Test
    public void testNEntityTableWithRemovalEmbeddingsLSH()
    {
        Table<String> table = TableParser.toTable(new File("testing/data/table-0001-1.json"));
        assertNotNull(table);

        Iterator<String> rowElements = table.getRow(0).iterator();
        List<String> row = new ArrayList<>();

        while (rowElements.hasNext())
        {
            row.add(rowElements.next());
        }

        Table<String> query = new DynamicTable<>(List.of(row));
        Iterator<Pair<String, Double>> results = this.embeddingsPrefilter.search(query).getResults();
        boolean foundQueryTable = false;

        while (results.hasNext())
        {
            if (results.next().getFirst().equals("table-0001-1.json"))
            {
                foundQueryTable = true;
            }
        }

        assertTrue("Query table was not returned", foundQueryTable);
    }
}
