package dk.aau.cs.daisy.edao.commands;

import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;
import dk.aau.cs.daisy.edao.connector.DBDriver;
import dk.aau.cs.daisy.edao.connector.SQLite;
import picocli.CommandLine;

import java.io.*;
import java.sql.ResultSet;

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
            SQLite db = SQLite.init(DB_NAME, DB_PATH);
            setupDBTable(db);
            insertEmbeddings(db, parser);

            return 0;
        }

        catch (IOException exception)
        {
            System.err.println("File error: " + exception.getMessage());
        }

        catch (Exception e)
        {
            System.err.println("SQLite exception: " + e.getMessage());
        }

        return -1;
    }

    private static void setupDBTable(DBDriver db)
    {
        db.update("CREATE TABLE Embeddings (" +
                            "entityIRI VARCHAR(100) NOT NULL," +
                            "embedding FLOAT[] NOT NULL);");
    }

    private static void insertEmbeddings(SQLite db, EmbeddingsParser parser)
    {
        String entity = null;
        StringBuilder embeddingBuilder = null;

        while (parser.hasNext())
        {
            EmbeddingsParser.EmbeddingToken token = parser.next();

            if (token.getToken() == EmbeddingsParser.EmbeddingToken.Token.ENTITY)
            {
                if (entity != null)
                    db.update("INSERT INTO Embeddings (entityIRI, embedding) VALUES ('" + entity +
                            "', '" + embeddingBuilder.toString().substring(0, embeddingBuilder.length() - 2) + "}');");

                entity = token.getLexeme();
                embeddingBuilder = new StringBuilder();
            }

            else
                embeddingBuilder.append(token.getLexeme()).append(", ");
        }

        db.update("INSERT INTO Embeddings (entityIRI, embedding) VALUES ('" + entity +
                "', '" + embeddingBuilder.toString().substring(0, embeddingBuilder.length() - 2) + "}');");
    }
}
