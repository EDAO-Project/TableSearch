package dk.aau.cs.daisy.edao.commands;

import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.commands.parser.ParsingException;
import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.SQLite;
import picocli.CommandLine;

import java.io.*;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

@picocli.CommandLine.Command(name = "embedding", description = "Loads embedding vectors into an SQLite database")
public class LoadEmbedding extends Command
{
    private static final String DB_NAME = "embeddings.db";
    private static final String DB_PATH = "./";

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

    @Override
    public Integer call()
    {
        try
        {
            EmbeddingsParser parser = new EmbeddingsParser(new FileInputStream(this.embeddingsFile));
            log("Parsing...");
            //parseFile(new FileInputStream(this.embeddingsFile));
            log("Parsing complete");

            SQLite db = SQLite.init(DB_NAME, DB_PATH);
            setupDBTable(db);
            int batchSize = 1000, batchSizeCount = batchSize;
            double mbLoaded = 0;

            while (parser.hasNext())
            {
                mbLoaded += (double) insertEmbeddings(db, parser, batchSize) / Math.pow(1024, 2);
                log("LOAD BATCH [" + batchSizeCount + "] - " + mbLoaded + "MB");
                batchSizeCount += batchSize;
            }

            db.close();
            return 0;
        }

        catch (IOException exception)
        {
            log("File error: " + exception.getMessage());
        }

        catch (ParsingException exc)
        {
            log("Parsing error: " + exc.getMessage());
        }

        return -1;
    }

    private static void log(String message)
    {
        System.out.println((new Date()).toString() + ": " + message);
    }

    private static void parseFile(InputStream inputStream)
    {
        EmbeddingsParser parser = new EmbeddingsParser(inputStream);

        while (parser.hasNext())
        {
            parser.next();
        }
    }

    private static void setupDBTable(DBDriver db)
    {
        db.update("CREATE TABLE Embeddings (" +
                            "entityIRI VARCHAR(100) NOT NULL," +
                            "embedding FLOAT[] NOT NULL);");
    }

    private static int insertEmbeddings(SQLite db, EmbeddingsParser parser, int batchSize)
    {
        String entity = null;
        StringBuilder embeddingBuilder = null,
                insertQuery = new StringBuilder("INSERT INTO Embeddings (entityIRI, embedding) VALUES ");
        int count = 0;
        EmbeddingsParser.EmbeddingToken prev = parser.prev(), token;

        if (prev != null && prev.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
        {
            entity = prev.getLexeme();
            embeddingBuilder = new StringBuilder();
            count++;
        }

        while (parser.hasNext() && count < batchSize && (token = parser.next()) != null)
        {
            if (token.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
            {
                if (entity != null)
                    insertQuery.append("('").append(entity).append("', '").
                                            append(embeddingBuilder.substring(0, embeddingBuilder.length() - 2)).
                                            append("}'), ");

                entity = token.getLexeme();
                embeddingBuilder = new StringBuilder();
                count++;
            }

            else
                embeddingBuilder.append(token.getLexeme()).append(", ");
        }

        String cleanedQuery = insertQuery.insert(insertQuery.length() - 2, ';').substring(0, insertQuery.length() - 2);
        db.update(cleanedQuery);
        return cleanedQuery.length();
    }
}
