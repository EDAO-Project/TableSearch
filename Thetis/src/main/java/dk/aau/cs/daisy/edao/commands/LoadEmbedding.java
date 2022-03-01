package dk.aau.cs.daisy.edao.commands;

import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.commands.parser.ParsingException;
import dk.aau.cs.daisy.edao.connector.*;
import picocli.CommandLine;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@picocli.CommandLine.Command(name = "embedding", description = "Loads embedding vectors into an SQLite database")
public class LoadEmbedding extends Command
{
    private static final char DELIMITER = ' ';
    private String dbPath = "./";

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    private File embeddingsFile = null;

    @CommandLine.Option(names = {"-f", "--file"}, description = "File with embeddings", required = true)
    public void setEmbeddingsFile(File value)
    {
        if (!value.exists())
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "File '" + value.getName() + "' does not exist");

        this.embeddingsFile = value;
    }

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output path of database instance (only required for SQLite and Milvus)")
    public void setOutputPath(String path)
    {
        this.dbPath = path;
    }

    @CommandLine.Option(names = {"-dp", "--disable-parsing"}, description = "Disables pre-parsing of embeddings file", defaultValue = "true")
    private boolean doParse;

    @CommandLine.Option(names = {"-h", "--host"}, description = "Host name of running Milvus or Postgres server")
    private String host = null;

    @CommandLine.Option(names = {"-p", "--port"}, description = "Port of running Milvus or Postgres server")
    private int port = -1;

    @CommandLine.Option(names = {"-dim", "--dimension"}, description = "Embeddings vector dimension (only required for Milvus)")
    private int dimension = -1;

    @CommandLine.Option(names = {"-db", "--database"}, description = "Type of database to store embeddings (sqlite, postgres, milvus)", required = true)
    private String dbType;

    @CommandLine.Option(names = {"-dbn", "--database-name"}, description = "Database name (only required for SQLite and Postgres)")
    private String dbName = null;

    @CommandLine.Option(names = {"-u", "--username"}, description = "Postgres username")
    private String psUsername = null;

    @CommandLine.Option(names = {"-pw", "--password"}, description = "Postgres password")
    private String psPassword = null;

    @Override
    public Integer call()
    {
        try
        {
            if (this.doParse)
            {
                log("Parsing...");
                parseFile(new FileInputStream(this.embeddingsFile));
                log("Parsing complete");
            }

            DBDriver<?, ?> db;
            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(this.embeddingsFile), DELIMITER);

            if (this.dbType.equals("sqlite"))
            {
                if (this.dbName == null)
                {
                    System.err.println("Missing DB name for SQLite");
                    return 1;
                }

                db = SQLite.init(this.dbName, this.dbPath);
            }

            else if (this.dbType.equals("postgres"))
            {
                if (this.dbName == null)
                {
                    System.err.println("Missing DB name for Postgres");
                    return 1;
                }

                else if (this.host == null || this.port == -1)
                {
                    System.err.println("Missing service info (hostname and/or port) for Postgres");
                    return 1;
                }

                else if (this.psUsername == null || this.psPassword == null)
                {
                    System.err.println("Missing login info for Postgres");
                    return 1;
                }

                db = Postgres.init(this.host, this.port, this.dbName, this.psUsername, this.psPassword);
            }

            else if (this.dbType.equals("milvus"))
            {
                if (this.host == null || this.port == -1)
                {
                    System.err.println("Missing service info (hostname and/or port) for Milvus");
                    return 1;
                }

                else if (this.dimension == -1)
                {
                    System.err.println("Missing vector dimension");
                    return 1;
                }

                db = new EmbeddingStore(this.dbPath, this.host, this.port, this.dimension);
            }

            else
            {
                System.err.println("Could not setup database instance");
                return 1;
            }

            EmbeddingDBWrapper wrapper = new EmbeddingDBWrapper(db, true);
            int batchSize = 100, batchSizeCount = batchSize;
            double loaded = 0;

            while (parser.hasNext())
            {
                int bytes = insertEmbeddings(wrapper, parser, batchSize);
                loaded += (double) bytes / Math.pow(1024, 2);

                if (bytes == 0)
                    log("INSERTION ERROR");

                else
                    log("LOAD BATCH [" + batchSizeCount + "] - " + loaded + " mb");

                batchSizeCount += batchSize;
            }

            wrapper.close();
            return 0;
        }

        catch (IOException exception)
        {
            log("File error: " + exception.getMessage());
        }

        catch (ParsingException exception)
        {
            log("Parsing error: " + exception.getMessage());
        }

        return -1;
    }

    private static void log(String message)
    {
        System.out.println(new Date() + ": " + message);
    }

    private static void parseFile(InputStream inputStream)
    {
        EmbeddingsParser parser = new EmbeddingsParser(inputStream, DELIMITER);

        while (parser.hasNext())
        {
            parser.next();
        }
    }

    private static int insertEmbeddings(DBDriverEmbedding<?, ?> db, EmbeddingsParser parser, int batchSize)
    {
        String entity = null;
        List<List<Float>> vectors = new ArrayList<>(batchSize);
        List<Float> embedding = new ArrayList<>();
        List<String> iris = new ArrayList<>(batchSize);
        int count = 0, loaded = 0;
        EmbeddingsParser.EmbeddingToken prev = parser.prev(), token;

        if (prev != null && prev.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
        {
            entity = prev.getLexeme();
            iris.add(entity);
            count++;
            loaded = entity.length() + 1;
        }

        while (parser.hasNext() && count < batchSize && (token = parser.next()) != null)
        {
            if (token.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
            {
                if (entity != null)
                    vectors.add(new ArrayList<>(embedding));

                entity = token.getLexeme();
                iris.add(entity);
                embedding.clear();
                count++;
                loaded += entity.length() + 1;
            }

            else
            {
                String lexeme = token.getLexeme();
                embedding.add(Float.parseFloat(lexeme));
                loaded += lexeme.length() + 1;
            }
        }

        if (!iris.isEmpty())
            iris.remove(iris.size() - 1);

        return db.batchInsert(iris, vectors) ? loaded : 0;
    }
}
