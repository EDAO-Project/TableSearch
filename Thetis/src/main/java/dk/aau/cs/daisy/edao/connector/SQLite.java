package dk.aau.cs.daisy.edao.connector;

import java.sql.*;

public class SQLite implements DBDriver
{
    private String errorMsg = null;
    private boolean error = false;
    private Connection connection;

    public static SQLite init(String dbName, String path)
    {
        SQLite db = new SQLite(path + dbName);

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
            setError(e.getMessage());
            return false;
        }
    }

    // Returns null of no error has occurred
    public String getError()
    {
        return this.error ? this.errorMsg : null;
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
            setError(e.getMessage());
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
            setError(e.getMessage());
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
            setError(e.getMessage());
            return false;
        }
    }

    private void setError(String msg)
    {
        this.error = true;
        this.errorMsg = msg;
    }
}