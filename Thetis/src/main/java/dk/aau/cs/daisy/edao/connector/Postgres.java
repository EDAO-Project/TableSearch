package dk.aau.cs.daisy.edao.connector;

import java.sql.*;

public class Postgres implements DBDriver<ResultSet, String>
{
    private Connection connection;

    public static Postgres init(String host, int port, String dbName, String user, String password)
    {
        return new Postgres(host, port, dbName, user, password);
    }

    private Postgres(String host, int port, String dbName, String user, String password)
    {
        try
        {
            Class.forName("org.postgresql.Driver");
            this.connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbName,
                    user,
                    password);
            this.connection.setAutoCommit(false);
        }

        catch (SQLException | ClassNotFoundException exception)
        {
            throw new IllegalArgumentException("Exception when initializing Postgres: " + exception.getMessage());
        }
    }

    /**
     * Standard SQL query execution
     * Returned ResultSet must be closed manually by the client
     * @param query String SQL query
     * @return ResultSet to be closed manually by the client
     */
    @Override
    public ResultSet select(String query)
    {
        try
        {
            Statement stmt = this.connection.createStatement();
            return stmt.executeQuery(query);
        }

        catch (SQLException e)
        {
            return null;
        }
    }

    /**
     * Standard SQL update query, such as insert, delete, alter, etc
     * @param query SQL query for updates
     * @return True if successful
     */
    @Override
    public boolean update(String query)
    {
        try
        {
            Statement stmt = this.connection.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
            this.connection.commit();

            return true;
        }

        catch (SQLException e)
        {
            return false;
        }
    }

    /**
     * Schema SQL update query
     * @param query Schema update query
     * @return True if update was successful
     */
    @Override
    public boolean updateSchema(String query)
    {
        return update(query);
    }

    /**
     * Closes Postgres connection
     * @return True if closing connection was successful
     */
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
            return false;
        }
    }

    /**
     * SQL drop query
     * @param query SQL query to drop tables
     * @return True if table drop was successful
     */
    @Override
    public boolean drop(String query)
    {
        return update(query);
    }
}
