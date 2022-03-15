package dk.aau.cs.daisy.edao.connector;

import dk.aau.cs.daisy.edao.connector.embeddings.EmbeddingDBWrapper;
import dk.aau.cs.daisy.edao.connector.embeddings.EmbeddingStore;
import dk.aau.cs.daisy.edao.connector.embeddings.RelationalEmbeddings;

import java.sql.ResultSet;
import java.util.List;

public class Factory
{
    public static DBDriver<ResultSet, String> makeRelational(String dbPath, String dbName)
    {
        return SQLite.init(dbName, dbPath);
    }

    public static DBDriver<ResultSet, String> makeRelational(String dbName)
    {
        return makeRelational(dbName, ".");
    }

    public static DBDriver<ResultSet, String> makeRelational(String host, int port, String dbName, String user, String password)
    {
        return Postgres.init(host, port, dbName, user, password);
    }

    public static DBDriverBatch<List<Double>, String> makeRelational(DBDriver<ResultSet, String> driver, boolean doSetup)
    {
        RelationalEmbeddings relational = new RelationalEmbeddings(driver);

        if (doSetup)
            relational.setup();

        return relational;
    }

    public static DBDriverBatch<List<Double>, String> makeVectorized(String host, int port, String dbPath, int vectorDimension)
    {
        return new EmbeddingStore(dbPath, host, port, vectorDimension);
    }

    public static EmbeddingDBWrapper wrap(DBDriver<?, ?> driver, boolean doSetup)
    {
        return new EmbeddingDBWrapper(driver, doSetup);
    }
}
