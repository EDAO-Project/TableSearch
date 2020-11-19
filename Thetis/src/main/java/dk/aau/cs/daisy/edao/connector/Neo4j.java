package dk.aau.cs.daisy.edao.connector;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import java.io.*;
import java.util.Properties;


public class Neo4j implements AutoCloseable {
    private final Driver driver;
    private final String dbUri;
    private final String dbUser;
    private final String dbPassword;

    public Neo4j(final String pathToConfigurationFile) throws IOException {
        this(new File(pathToConfigurationFile));
    }

    public Neo4j(final File confFile) throws IOException {
        Properties prop = new Properties();
        InputStream inputStream;

        if (confFile.exists()) {
            inputStream = new FileInputStream(confFile);
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + confFile.getAbsolutePath() + "' not found");
        }

        this.dbUri = prop.getProperty("neo4j.uri", "bolt://localhost:7687");
        this.dbUser = prop.getProperty("neo4j.user", "neo4j");
        this.dbPassword = prop.getProperty("neo4j.password", "admin");
        this.driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
    }


    public Neo4j(String uri, String user, String password) {
        this.dbUri = uri;
        this.dbUser = user;
        this.dbPassword = password;
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }


    @Override
    public void close() {
        driver.close();
    }

    public void testConnection() {
        try (Session session = driver.session()) {
            String greeting = session.readTransaction(tx -> {
                Result result = tx.run("MATCH (a:Resource) " +
                        "RETURN a.uri LIMIT 10");
                return result.single().get(0).asString();
            });
            System.out.println(greeting);
        }
    }
}
