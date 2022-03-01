package dk.aau.cs.daisy.edao.connector;

import java.sql.*;
import java.util.List;

public class SQLite implements DBDriver<ResultSet, String>, ExplainableCause
{
    private String errorMsg = null, stackTrace = null;
    private boolean error = false;
    private Connection connection;

    public static SQLite init(String dbName, String path)
    {
        SQLite db = new SQLite(path + (path.endsWith("/") ? "" : "/") + dbName);

        if (db.error)
            throw new RuntimeException("Could not open database: " + db.getError());

        return db;
    }

    public static SQLite init(String dbName)
    {
        return init(dbName, "./");
    }

    private SQLite(String dbName)
    {
        openDB(dbName);
    }

    private boolean openDB(String dbName)
    {
        try
        {
            Class.forName("org.sqlite.JDBC");
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
            this.connection.setAutoCommit(false);
            return true;
        }

        catch (ClassNotFoundException | SQLException e)
        {
            setError(e.getMessage(), e.getStackTrace());
            return false;
        }
    }

    // Returns null of no error has occurred
    @Override
    public String getError()
    {
        return this.error ? this.errorMsg : null;
    }

    @Override
    public String getStackTrace()
    {
        return this.error ? this.stackTrace : null;
    }

    @Override
    public boolean update(String query)
    {
        try
        {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(query);
            statement.close();
            this.connection.commit();

            return true;
        }

        catch (SQLException e)
        {
            setError(e.getMessage(), e.getStackTrace());
            return false;
        }
    }

    // ResultSet must be closed manually by client
    @Override
    public ResultSet select(String query)
    {
        try
        {
            Statement statement = this.connection.createStatement();
            ResultSet rs = statement.executeQuery(query);

            return rs;
        }

        catch (SQLException e)
        {
            setError(e.getMessage(), e.getStackTrace());
            return null;
        }
    }

    @Override
    public boolean updateSchema(String query)
    {
        return update(query);
    }

    @Override
    public boolean close()
    {
        try
        {
            this.connection.close();
            return true;
        }

        catch (SQLException e)
        {
            setError(e.getMessage(), e.getStackTrace());
            return false;
        }
    }

    @Override
    public boolean drop(String query)
    {
        throw new UnsupportedOperationException("Dropping tables is not yet supported");
    }

    public boolean migrate(List<String> tableNames, String dbName, String path)
    {
        try
        {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path + dbName);
            conn.setAutoCommit(false);

            if (!insertTables(conn, tableNames))
                return false;

            close();
            this.connection = conn;
            return true;
        }

        catch (ClassNotFoundException | SQLException exc)
        {
            setError(exc.getMessage(), exc.getStackTrace());
            return false;
        }
    }

    private boolean insertTables(Connection conn, List<String> tables)
    {
        try
        {
            Statement statement = conn.createStatement();

            for (String table : tables)
            {
                statement.executeUpdate("CREATE TABLE " + table + " AS SELECT * FROM " + table + ";");
            }

            return true;
        }

        catch (SQLException exc)
        {
            setError(exc.getMessage(), exc.getStackTrace());
            return false;
        }
    }

    private void setError(String msg, StackTraceElement[] stackTrace)
    {
        this.error = true;
        this.errorMsg = msg;
        this.stackTrace = "";

        for (StackTraceElement elem : stackTrace)
        {
            this.stackTrace += elem.toString() + "\n";
        }
    }
}
