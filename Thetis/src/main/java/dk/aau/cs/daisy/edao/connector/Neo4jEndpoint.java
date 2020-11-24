package dk.aau.cs.daisy.edao.connector;

import dk.aau.cs.daisy.edao.structures.Pair;
import org.neo4j.driver.*;

import java.io.*;
import java.util.*;


public class Neo4jEndpoint implements AutoCloseable {
    private final Driver driver;
    private final String dbUri;
    private final String dbUser;
    private final String dbPassword;

    public Neo4jEndpoint(final String pathToConfigurationFile) throws IOException {
        this(new File(pathToConfigurationFile));
    }

    public Neo4jEndpoint(final File confFile) throws IOException {
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


    public Neo4jEndpoint(String uri, String user, String password) {
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
            Long numNodes = session.readTransaction(tx -> {
                Result result = tx.run("MATCH (a:Resource) " +
                        "RETURN COUNT(a) as count");
                return result.single().get("count").asLong();
            });
            System.out.printf("Connection established. Nodes: %d ", numNodes);
        }
    }

    public List<String> searchLinks(Iterable<String> links) {


        Map<String, Object> params = new HashMap<>();

        params.put("linkList", links);

        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<String> entityUris = new ArrayList<>();
                Result result = tx.run("MATCH (a:Resource) -[l:ns57__isPrimaryTopicOf]-> (b:Resource)" + "\n"
                        + "WHERE b.uri in $linkList" + "\n"
                        + "RETURN a.uri as mention", params);

                for (Record r : result.list()) {
                    entityUris.add(r.get("mention").asString());
                }
                return entityUris;
            });
        }


    }

    public List<Pair<String, String>> searchLinkMentions(List<String> links) {

        Map<String, Object> params = new HashMap<>();

        params.put("linkList", links);

        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Pair<String, String>> entityUris = new ArrayList<>();
                Result result = tx.run("MATCH (a:Resource) -[l:ns57__isPrimaryTopicOf]-> (b:Resource)"
                        + "WHERE b.uri in $linkList"
                        + "RETURN a.uri as uri1, b.uri as uri2", params);
                for (Record r : result.list()) {
                    entityUris.add(new Pair<>(r.get("uri1").asString(), r.get("uri2").asString()));
                }
                return entityUris;
            });
        }


    }
}
