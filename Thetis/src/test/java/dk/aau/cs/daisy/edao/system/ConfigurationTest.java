package dk.aau.cs.daisy.edao.system;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConfigurationTest
{
    @After
    public void reset()
    {
        (new File(".config.conf")).delete();
    }

    @Test
    public void testReadNotExists()
    {
        assertNull(Configuration.getDBHost());
    }

    @Test
    public void testDBHost()
    {
        Configuration.setDBHost("host");
        assertEquals("host", Configuration.getDBHost());
    }

    @Test
    public void testDBPort()
    {
        Configuration.setDBPort(1234);
        assertEquals(1234, Configuration.getDBPort());
    }

    @Test
    public void testDBUsername()
    {
        Configuration.setDBUsername("username");
        assertEquals("username", Configuration.getDBUsername());
    }

    @Test
    public void testDBPassword()
    {
        Configuration.setDBPassword("password");
        assertEquals("password", Configuration.getDBPassword());
    }

    @Test
    public void testDBType()
    {
        Configuration.setDB("db");
        assertEquals("db", Configuration.getDB());
    }

    @Test
    public void testDBName()
    {
        Configuration.setDBName("name");
        assertEquals("name", Configuration.getDBName());
    }

    @Test
    public void testDBPath()
    {
        Configuration.setDBPath("path");
        assertEquals("path", Configuration.getDBPath());
    }

    @Test
    public void testDBEmbeddingsDimension()
    {
        Configuration.setEmbeddingsDimension(100);
        assertEquals(100, Configuration.getEmbeddingsDimension());
    }
}
