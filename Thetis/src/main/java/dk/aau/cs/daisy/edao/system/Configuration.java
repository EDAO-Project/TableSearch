package dk.aau.cs.daisy.edao.system;

import java.io.*;
import java.util.Properties;

public class Configuration
{
    private static class ConfigurationIO
    {
        private InputStream input;
        private OutputStream output;

        ConfigurationIO(InputStream input)
        {
            this.input = input;
            this.output = null;
        }

        ConfigurationIO(OutputStream output)
        {
            this.output = output;
            this.input = null;
        }

        void save(Properties properties)
        {
            if (this.output == null)
                throw new UnsupportedOperationException("No output stream class provided");

            try (ObjectOutputStream objectOutput = new ObjectOutputStream(this.output))
            {
                objectOutput.writeObject(properties);
                objectOutput.flush();
            }

            catch (IOException e)
            {
                throw new RuntimeException("IOException when saving configuration: " + e.getMessage());
            }
        }

        Properties read()
        {
            if (this.input == null)
                throw new UnsupportedOperationException("No input stream class provided");

            try (ObjectInputStream objectInput = new ObjectInputStream(this.input))
            {
                return (Properties) objectInput.readObject();
            }

            catch (IOException | ClassNotFoundException e)
            {
                throw new RuntimeException("Exception when reading configuration: " + e.getMessage());
            }
        }
    }

    private static final File CONF_FILE = new File(".config.conf");

    static
    {
        addDefaults();
    }

    private static void addDefaults()
    {
        Properties props = readProperties();

        if (!props.contains("EntityTable"))
            props.setProperty("EntityTable", "entity_table.ser");

        if (!props.contains("EntityLinker"))
            props.setProperty("EntityLinker", "entity_linker.ser");

        if (!props.contains("EntityToTables"))
            props.setProperty("EntityToTables", "entity_to_tables.ser");

        if (!props.contains("TableToEntities"))
            props.setProperty("TableToEntities", "tableIDToEntities.ttl");

        if (!props.contains("TableToTypes"))
            props.setProperty("TableToTypes", "tableIDToTypes.ttl");

        if (!props.contains("WikiLinkToEntitiesFrequency"))
            props.setProperty("WikiLinkToEntitiesFrequency", "wikilinkToNumEntitiesFrequency.json");

        if (!props.contains("CellToNumLinksFrequency"))
            props.setProperty("CellToNumLinksFrequency", "cellToNumLinksFrequency.json");

        if (!props.contains("TableStats"))
            props.setProperty("TableStats", "statistics/perTableStats.json");

        writeProperties(props);
    }

    private static Properties readProperties()
    {
        try
        {
            return (new ConfigurationIO(new FileInputStream(CONF_FILE))).read();
        }

        catch (FileNotFoundException | RuntimeException e)
        {
            return new Properties();
        }
    }

    private static void writeProperties(Properties properties)
    {
        try
        {
            (new ConfigurationIO(new FileOutputStream(CONF_FILE))).save(properties);
        }

        catch (FileNotFoundException e) {}
    }

    private static void addProperty(String key, String value)
    {
        Properties properties = readProperties();
        properties.setProperty(key, value);
        writeProperties(properties);
    }

    public static void setDB(String db)
    {
        addProperty("db", db);
    }

    public static String getDB()
    {
        return readProperties().getProperty("db");
    }

    public static void setDBPath(String path)
    {
        addProperty("DBPath", path);
    }

    public static String getDBPath()
    {
        return readProperties().getProperty("DBPath");
    }

    public static void setDBName(String name)
    {
        addProperty("DBName", name);
    }

    public static String getDBName()
    {
        return readProperties().getProperty("DBName");
    }

    public static void setDBHost(String host)
    {
        addProperty("DBHost", host);
    }

    public static String getDBHost()
    {
        return readProperties().getProperty("DBHost");
    }

    public static void setDBPort(int port)
    {
        addProperty("DBPort", String.valueOf(port));
    }

    public static int getDBPort()
    {
        return Integer.parseInt(readProperties().getProperty("DBPort"));
    }

    public static void setEmbeddingsDimension(int dimension)
    {
        addProperty("EmbeddingsDim", String.valueOf(dimension));
    }

    public static int getEmbeddingsDimension()
    {
        return Integer.parseInt(readProperties().getProperty("EmbeddingsDim"));
    }

    public static void setDBUsername(String username)
    {
        addProperty("DBUsername", username);
    }

    public static String getDBUsername()
    {
        return readProperties().getProperty("DBUsername");
    }

    public static void setDBPassword(String password)
    {
        addProperty("DBPassword", password);
    }

    public static String getDBPassword()
    {
        return readProperties().getProperty("DBPassword");
    }

    public static void setLargestId(String id)
    {
        addProperty("LargestID", id);
    }

    public static String getLargestId()
    {
        return readProperties().getProperty("LargestID");
    }

    public static String getEntityTableFile()
    {
        return readProperties().getProperty("EntityTable");
    }

    public static String getEntityLinkerFile()
    {
        return readProperties().getProperty("EntityLinker");
    }

    public static String getEntityToTablesFile()
    {
        return readProperties().getProperty("EntityToTables");
    }

    public static String getTableToEntitiesFile()
    {
        return readProperties().getProperty("TableToEntities");
    }

    public static String getTableToTypesFile()
    {
        return readProperties().getProperty("TableToTypes");
    }

    public static String getWikiLinkToEntitiesFrequencyFile()
    {
        return readProperties().getProperty("WikiLinkToEntitiesFrequency");
    }

    public static String getCellToNumLinksFrequencyFile()
    {
        return readProperties().getProperty("CellToNumLinksFrequency");
    }

    public static String getTableStatsFile()
    {
        return readProperties().getProperty("TableStats");
    }
}
