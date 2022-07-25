package dk.aau.cs.daisy.edao.commands;

import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.commands.parser.ParsingException;
import dk.aau.cs.daisy.edao.connector.*;
import dk.aau.cs.daisy.edao.system.Configuration;
import dk.aau.cs.daisy.edao.system.Logger;
import picocli.CommandLine;

import java.io.*;
import java.util.ArrayList;
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

    @CommandLine.Option(names = {"-db", "--database"}, description = "Type of database to store embeddings (sqlite, postgres, milvus, relational_wrap)", required = true)
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
            saveParams();

            if (this.doParse)
            {
                Logger.logNewLine(Logger.Level.INFO, "Parsing...");
                parseFile(new FileInputStream(this.embeddingsFile));
                Logger.logNewLine(Logger.Level.INFO, "Parsing complete");
            }

            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(this.embeddingsFile), DELIMITER);
            DBDriverBatch<List<Double>, String> db = Factory.fromConfig(true);
            int batchSize = 100, batchSizeCount = batchSize;
            double loaded = 0;

            while (parser.hasNext())
            {
                int bytes = insertEmbeddings(db, parser, batchSize);
                loaded += (double) bytes / Math.pow(1024, 2);

                if (bytes == 0)
                    Logger.logNewLine(Logger.Level.ERROR, "INSERTION ERROR: " + ((ExplainableCause) db).getError());

                else
                    Logger.log(Logger.Level.INFO, "LOAD BATCH [" + batchSizeCount + "] - " + loaded + " mb");

                batchSizeCount += bytes > 0 ? batchSize : 0;
            }

            db.close();
            return 0;
        }

        catch (IOException exception)
        {
            Logger.logNewLine(Logger.Level.ERROR, "File error: " + exception.getMessage());
        }

        catch (ParsingException exception)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Parsing error: " + exception.getMessage());
        }

        return -1;
    }

    private void saveParams()
    {
        Configuration.setDB(this.dbType);
        Configuration.setDBPath(this.dbPath);

        if (this.host != null)
            Configuration.setDBHost(this.host);

        if (this.port != -1)
            Configuration.setDBPort(this.port);

        if (this.dimension != -1)
            Configuration.setEmbeddingsDimension(this.dimension);

        if (this.dbName != null)
            Configuration.setDBName(this.dbName);

        if (this.psUsername != null)
            Configuration.setDBUsername(this.psUsername);

        if (this.psPassword != null)
            Configuration.setDBPassword(this.psPassword);
    }

    private static void parseFile(InputStream inputStream)
    {
        EmbeddingsParser parser = new EmbeddingsParser(inputStream, DELIMITER);

        while (parser.hasNext())
        {
            parser.next();
        }
    }

    private static int insertEmbeddings(DBDriverBatch<?, ?> db, EmbeddingsParser parser, int batchSize)
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
