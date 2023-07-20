package com.thetis.loader;

import com.thetis.connector.Neo4jEndpoint;
import com.thetis.system.Configuration;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class LuceneLinker implements Linker
{
    private IndexSearcher searcher;
    private File kgDir;
    private Neo4jEndpoint neo4j;
    private final QueryParser parser = new QueryParser(TEXT_FIELD, new StandardAnalyzer());
    private final Set<String> entities = new HashSet<>(); // URIs
    private static final int CACHE_MAX = 10000;
    private final Map<String, String> cache =
            Collections.synchronizedMap(new LinkedHashMap<>(CACHE_MAX, 0.75f, true) {
        @Override
        public boolean removeEldestEntry(Map.Entry eldest)
        {
            return size() > CACHE_MAX;
        }
    });
    public static final String URI_FIELD = "uri";
    public static final String TEXT_FIELD = "text";

    public LuceneLinker(File kgDir, boolean construct)
    {
        this(null, kgDir, construct);
    }

    public LuceneLinker(Neo4jEndpoint neo4j, File kgDir, boolean construct)
    {
        this.neo4j = neo4j;
        this.kgDir = kgDir;
        setup(construct);
    }

    private void setup(boolean construct)
    {
        Path luceneDir = new File(Configuration.getLuceneDir()).toPath();

        if (construct)
        {
            construct(luceneDir);
        }

        load(luceneDir);
    }

    private void construct(Path luceneDir)
    {
        try (Analyzer analyzer = new StandardAnalyzer())
        {
            prepare(luceneDir);

            Directory directory = FSDirectory.open(luceneDir);
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            IndexWriter writer = new IndexWriter(directory, config);

            for (File kgFile : Objects.requireNonNull(this.kgDir.listFiles(f -> f.getName().endsWith(".ttl"))))
            {
                loadEntities(kgFile);
            }

            if (this.neo4j == null)
            {
                loadFromFiles(writer);
            }

            else
            {
                loadFromNeo4J(writer);
            }

            writer.close();
            directory.close();
        }

        catch (FileNotFoundException e)
        {
            throw new RuntimeException("Could not find KG file: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when loading Lucene entity linker: " + e.getMessage());
        }
    }

    private static void prepare(Path dir)
    {
        File folder = dir.toFile();

        if (folder.exists())
        {
            for (File f : folder.listFiles())
            {
                f.delete();
            }

            folder.delete();
        }

        folder.mkdir();
    }

    private void loadFromFiles(IndexWriter writer) throws FileNotFoundException
    {
        this.entities.forEach(uri -> {
            try
            {
                Document doc = new Document();
                doc.add(new Field(URI_FIELD, uri, TextField.TYPE_STORED));
                doc.add(new Field(TEXT_FIELD, uriPostfix(uri), TextField.TYPE_STORED));
                writer.addDocument(doc);
            }

            catch (IOException ignored) {}
        });
    }

    private void loadFromNeo4J(IndexWriter writer)
    {
        this.entities.forEach(uri -> {
            try
            {
                Document doc = new Document();
                String label = this.neo4j.searchLabel(uri);

                if (uri != null && label != null)
                {
                    doc.add(new Field(URI_FIELD, uri, TextField.TYPE_STORED));
                    doc.add(new Field(TEXT_FIELD, label, TextField.TYPE_STORED));
                    writer.addDocument(doc);
                }
            }

            catch (IOException ignored) {}
        });
    }

    private void loadEntities(File kgFile) throws FileNotFoundException
    {
        Model m = ModelFactory.createDefaultModel();
        m.read(new FileInputStream(kgFile), null, "TTL");
        ExtendedIterator<Triple> iter = m.getGraph().find();

        while (iter.hasNext())
        {
            Triple triple = iter.next();
            this.entities.add(triple.getSubject().getURI());
        }
    }

    private static String uriPostfix(String uri)
    {
        String[] uriSplit = uri.split("/");
        return uriSplit[uriSplit.length - 1].replace('_', ' ');
    }

    private void load(Path luceneDir)
    {
        try
        {
            Directory directory = FSDirectory.open(luceneDir);
            DirectoryReader reader = DirectoryReader.open(directory);
            this.searcher = new IndexSearcher(reader);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed loading Lucene entity linking indexes: " + e.getMessage());
        }
    }

    @Override
    public String link(String mention)
    {
        if (mention.contains("http"))
        {
            mention = uriPostfix(mention);
        }

        String link = this.cache.get(mention);

        if (link != null)
        {
            return link;
        }

        try
        {
            Query query = this.parser.parse(mention);
            ScoreDoc[] hits = this.searcher.search(query, 1).scoreDocs;

            if (hits.length == 0)
            {
                return null;
            }

            Document doc = this.searcher.doc(hits[0].doc);
            link = doc.get(TEXT_FIELD);
            this.cache.put(uriPostfix(link), link);
            return doc.get(URI_FIELD);
        }

        catch (ParseException | IOException e)
        {
            return null;
        }
    }
}
