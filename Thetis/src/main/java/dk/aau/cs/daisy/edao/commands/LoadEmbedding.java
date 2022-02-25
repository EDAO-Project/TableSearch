package dk.aau.cs.daisy.edao.commands;

import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.commands.parser.ParsingException;
import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.EmbeddingStore;
import picocli.CommandLine;

import java.io.*;
import java.util.Date;

@picocli.CommandLine.Command(name = "embedding", description = "Loads embedding vectors into an SQLite database")
public class LoadEmbedding extends Command
{
    private static final char DELIMITER = ' ';
    private static final String DB_NAME = "embeddings.db";
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

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output path of database instance")
    public void setOutputPath(String path)
    {
        this.dbPath = path;
    }

    @CommandLine.Option(names = {"-db", "--disable-parsing"}, description = "Disables pre-parsing of embeddings file", defaultValue = "true")
    private boolean doParse;

    @CommandLine.Option(names = {"-h", "--host"}, description = "Host name of running Milvus server", required = true)
    private String host;

    @CommandLine.Option(names = {"-p", "--port"}, description = "Port of running Milvus server", required = true)
    private int port;

    @CommandLine.Option(names = {"-dim", "--dimension"}, description = "Embeddings vector dimension", required = true)
    private int dimension;

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

            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(this.embeddingsFile), DELIMITER);
            EmbeddingStore store = new EmbeddingStore(this.dbPath, this.host, this.port, this.dimension);
            int batchSize = 100, batchSizeCount = batchSize;
            double loaded = 0;

            while (parser.hasNext())
            {
                int bytes = insertEmbeddings(store, parser, batchSize);
                loaded += (double) bytes / Math.pow(1024, 2);

                if (bytes == 0)
                    log("INSERTION ERROR");

                else
                    log("LOAD BATCH [" + batchSizeCount + "] - " + loaded + " mb");

                batchSizeCount += batchSize;
            }

            store.close();
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

    private static void setupDBTable(DBDriver db)
    {
        db.update("CREATE TABLE Embeddings (" +
                            "entityIRI VARCHAR(100) PRIMARY KEY," +
                            "embedding VARCHAR(1000) NOT NULL);");
    }

    private static int insertEmbeddings(EmbeddingStore db, EmbeddingsParser parser, int batchSize)
    {
        String entity = null;
        StringBuilder embeddingBuilder = null;
        int count = 0, loaded = 0;
        EmbeddingsParser.EmbeddingToken prev = parser.prev(), token;

        if (prev != null && prev.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
        {
            entity = prev.getLexeme();
            embeddingBuilder = new StringBuilder();
            count++;
            loaded = entity.length() + 1;
        }

        while (parser.hasNext() && count < batchSize && (token = parser.next()) != null)
        {
            if (token.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
            {
                if (entity != null)
                {
                    if (db.update(entity + " " + embeddingBuilder))
                        loaded += entity.length() + 1;
                }

                entity = token.getLexeme();
                embeddingBuilder = new StringBuilder();
                count++;
            }

            else
            {
                String lexeme = token.getLexeme();
                loaded += lexeme.length() + 1;
                embeddingBuilder.append(lexeme).append(",");
            }
        }

        return loaded;
    }
}
