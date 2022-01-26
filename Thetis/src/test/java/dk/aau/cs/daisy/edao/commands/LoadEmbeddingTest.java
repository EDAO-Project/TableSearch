package dk.aau.cs.daisy.edao.commands;

import org.junit.Test;

import dk.aau.cs.daisy.edao.connector.SQLite;
import dk.aau.cs.daisy.edao.commands.parser.EmbeddingsParser;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class LoadEmbeddingTest
{
    private static final String DB_PATH = "./";
    private static final String DB_NAME = "test.db";

    private static void loadDB(SQLite db)
    {
        db.updateSchema("CREATE TABLE Person (" +
                                "id INT PRIMARY KEY," +
                                "name VARCHAR(25) NOT NULL," +
                                "age INT NOT NULL);");

        db.update("INSERT INTO Person VALUES (1, 'Person1', 10);");
        db.update("INSERT INTO Person VALUES (2, 'Person2', 20);");
        db.update("INSERT INTO Person VALUES (3, 'Person3', 30);");
    }

    private static void clearDB()
    {
        File dbFile = new File(DB_PATH + DB_NAME);

        if (dbFile.exists())
            dbFile.delete();
    }

    @Test
    public void testParser()
    {
        EmbeddingsParser parser = new EmbeddingsParser("http://dbpedia.org/property/seats6Title -0.2933714 0.539444 -0.32271475 -0.83340573 0.8849156 0.24414709 -0.107573666");
        assertTrue(parser.hasNext());

        EmbeddingsParser.EmbeddingToken t = parser.next();
        assertEquals("http://dbpedia.org/property/seats6Title", t.getLexeme());
        assertEquals(EmbeddingsParser.EmbeddingToken.Token.ENTITY, t.getToken());

        for (int i = 0; i < 7; i++)
        {
            assertTrue(parser.hasNext());
            assertEquals(EmbeddingsParser.EmbeddingToken.Token.VALUE, parser.next().getToken());
        }
    }

    @Test
    public void testSQLiteOpen()
    {
        SQLite db = SQLite.init(DB_NAME);
        assertNull(db.getError());
        db.close();
        clearDB();
    }

    @Test
    public void createSchema()
    {
        SQLite db = SQLite.init(DB_NAME);
        loadDB(db);

        ResultSet rs = db.select("SELECT * FROM Person;");
        assertNotNull(rs);

        try
        {
            assertTrue(rs.next());
            assertEquals("Person1", rs.getString("name"));
            assertEquals(10, rs.getInt("age"));

            rs.close();
            db.close();
            clearDB();
        }

        catch (SQLException e)
        {
            db.close();
            clearDB();
            fail();
        }
    }
}
