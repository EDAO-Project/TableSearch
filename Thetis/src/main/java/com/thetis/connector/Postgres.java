package com.thetis.connector;

import java.sql.*;

public class Postgres implements DBDriver<ResultSet, String>, ExplainableCause
{
    private Connection connection;
    private boolean error = false;
    private String errorMsg, stackTrace;

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
            setError(exception.getMessage(), exception.getStackTrace());
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
            setError(e.getMessage(), e.getStackTrace());
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
            commit();

            return true;
        }

        catch (SQLException e)
        {
            setError(e.getMessage(), e.getStackTrace());
            commit();
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
            setError(e.getMessage(), e.getStackTrace());
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

    private boolean commit()
    {
        try
        {
            this.connection.commit();
            return true;
        }

        catch (SQLException exception)
        {
            setError(exception.getMessage(), exception.getStackTrace());
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

    @Override
    public String getError()
    {
        return error ? this.errorMsg : null;
    }

    @Override
    public String getStackTrace()
    {
        return error ? this.stackTrace : null;
    }
}
