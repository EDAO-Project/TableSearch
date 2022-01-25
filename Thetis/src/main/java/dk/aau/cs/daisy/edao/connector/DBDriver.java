package dk.aau.cs.daisy.edao.connector;

import java.sql.ResultSet;

public interface DBDriver
{
    ResultSet select(String query);
    boolean update(String query);
    boolean updateSchema(String query);
    boolean close();
}
