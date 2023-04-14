package dk.aau.cs.daisy.edao.connector;

import dk.aau.cs.daisy.edao.connector.embeddings.EmbeddingDBWrapper;
import dk.aau.cs.daisy.edao.connector.embeddings.EmbeddingStore;
import dk.aau.cs.daisy.edao.system.Configuration;

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
        return new EmbeddingDBWrapper(driver, doSetup);
    }

    public static DBDriverBatch<List<Double>, String> makeVectorized(String host, int port, String dbPath, int vectorDimension)
    {
        return new EmbeddingStore(dbPath, host, port, vectorDimension);
    }

    public static EmbeddingDBWrapper wrap(DBDriver<?, ?> driver, boolean doSetup)
    {
        return new EmbeddingDBWrapper(driver, doSetup);
    }

    public static DBDriverBatch<List<Double>, String> fromConfig(boolean doSetup)
    {
        String dbType = Configuration.getDB();

        if ("sqlite".equals(dbType))
            return wrap(SQLite.init(Configuration.getDBName(), Configuration.getDBPath()), doSetup);

        else if ("postgres".equals(dbType))
            return wrap(Postgres.init(Configuration.getDBHost(), Configuration.getDBPort(), Configuration.getDBName(),
                    Configuration.getDBUsername(), Configuration.getDBPassword()), doSetup);

        else if ("milvus".equals(dbType))
            return wrap(new EmbeddingStore(Configuration.getDBPath(), Configuration.getDBHost(), Configuration.getDBPort(),
                    Configuration.getEmbeddingsDimension()), doSetup);

        else if ("relational_wrap".equals(dbType))
        {
            if (Configuration.getDBHost() != null)
                return makeRelational(makeRelational(Configuration.getDBHost(), Configuration.getDBPort(),
                        Configuration.getDBName(), Configuration.getDBUsername(), Configuration.getDBPassword()), doSetup);

            else if (Configuration.getDBPath() != null)
                return makeRelational(makeRelational(Configuration.getDBPath(), Configuration.getDBName()), doSetup);

            else
                return makeRelational(makeRelational(Configuration.getDBName()), doSetup);
        }

        else throw new RuntimeException("Un-recognized DB type: '" + dbType + "'");
    }
}
