package dk.aau.cs.daisy.edao.connector;

import dk.aau.cs.daisy.edao.structures.Pair;
import org.neo4j.driver.*;

import java.io.*;
import java.util.*;

/**
 * Connects and query the KG in Neo4j
 */
public class Neo4jEndpoint implements AutoCloseable {
    private final Driver driver;
    private final String dbUri;
    private final String dbUser;
    private final String dbPassword;
    private final String isPrimaryTopicOf_rel_type_name;

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
        this.isPrimaryTopicOf_rel_type_name = this.get_isPrimaryTopicOf_rel_type_name();
    }


    public Neo4jEndpoint(String uri, String user, String password) {
        this.dbUri = uri;
        this.dbUser = user;
        this.dbPassword = password;
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.isPrimaryTopicOf_rel_type_name = this.get_isPrimaryTopicOf_rel_type_name();
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

    /**
     * @param none
     * @return a string with the name of the link corresponding to the isPrimaryTopicOf in the knowledgebase.
     * Return a null string if it is not found
     */
    public String get_isPrimaryTopicOf_rel_type_name() {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                // Get list of all relationship types (i.e. all link names)
                Result rel_types = tx.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType");
                String isPrimaryTopicOf_link_name = null;
                for (Record r : rel_types.list()) {
                    String rel_type = r.get("relationshipType").asString();
                    if (rel_type.contains("isPrimaryTopicOf")) {
                        isPrimaryTopicOf_link_name = rel_type;
                    }
                }
                return isPrimaryTopicOf_link_name;
            });
        }
    }


    /**
     *
     * @param links a list of wikipedia links [https://en.wikipedia.org/wiki/Yellow_Yeiyah, ...]
     * @return a list of mapped dbpedia links [http://dbpedia.org/resource/Yellow_Yeiyah, ...]
     */
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

    /**
     *
     * @param link a specific wikipedia link
     * @return a list of possible entity matches
     */
    public List<String> searchLink(String link) {

        Map<String, Object> params = new HashMap<>();
        params.put("link", link);

        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<String> entityUris = new ArrayList<>();

                // Get all entity uri given a wikipedia link
                Result result = tx.run("MATCH (a:Resource) -[l:"+this.isPrimaryTopicOf_rel_type_name+"]-> (b:Resource)" + "\n"
                        + "WHERE b.uri in [$link]" + "\n"
                        + "RETURN a.uri as mention", params);

                for (Record r : result.list()) {
                    entityUris.add(r.get("mention").asString());
                }
                return entityUris;
            });
        }
    }

    /**
     *
     * @param link a specific entity (i.e. a dbpedia link)
     * @return the list of rdf__type uris corresonding to the 
     */
    public List<String> searchTypes(String entity) {
        Map<String, Object> params = new HashMap<>();
        params.put("entity", entity);

        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<String> entity_types = new ArrayList<>();

                // Get all entity uri given a wikipedia link
                Result result = tx.run("MATCH (a:Resource) -[l:rdf__type]-> (b:Resource)" + "\n"
                        + "WHERE a.uri in [$entity]" + "\n"
                        + "RETURN b.uri as mention", params);

                for (Record r : result.list()) {
                    entity_types.add(r.get("mention").asString());
                }

                return entity_types;
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
