package dk.aau.cs.daisy.edao.connector.embeddings;

import dk.aau.cs.daisy.edao.connector.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationalEmbeddings implements DBDriverBatch<List<Double>, String>, Setup, ExplainableCause
{
    private DBDriver<ResultSet, String> driver;
    private boolean error = false;
    private String errMsg = null;
    private StackTraceElement[] stackTrace = null;

    private static final String HASHES = "Hash";
    private static final String HASH_FIELD = "hash";
    private static final String IRI_FIELD = "iri";
    private static final String ID_FIELD = "id";

    private static final String EMBEDDINGS = "Embeddings";
    private static final String EMBEDDING_FIELD = "embedding";

    // We don't need to check driver for being for relational DBs, since this is implied by its generic types
    public RelationalEmbeddings(DBDriver<ResultSet, String> driver)
    {
        if (!(driver instanceof ExplainableCause))     // In case we update the relational database driver classes
            throw new IllegalArgumentException("Driver cannot explain errors");

        this.driver = driver;
    }

    @Override
    public void setup()
    {
        boolean val1 = this.driver.updateSchema("CREATE TABLE IF NOT EXISTS " + HASHES + " (" +
                                                            HASH_FIELD + " INTEGER NOT NULL," +
                                                            IRI_FIELD + " VARCHAR(1000) NOT NULL," +
                                                            ID_FIELD + " INTEGER PRIMARY KEY " +
                                                                        (this.driver instanceof SQLite ?
                                                                                " AUTOINCREMENT" :
                                                                                " AUTO_INCREMENT") + ");"),
                val2 = this.driver.updateSchema("CREATE TABLE IF NOT EXISTS " + EMBEDDINGS + " (" +
                                                            ID_FIELD + " INTEGER PRIMARY KEY," +
                                                            EMBEDDING_FIELD + " VARCHAR(1000) NOT NULL);");

        if (!val1 || !val2)
        {
            this.error = true;
            this.errMsg = ((ExplainableCause) this.driver).getError();
        }

        boolean val3 = this.driver.update("CREATE INDEX idx ON " + HASHES + " (" + HASH_FIELD + ");");

        if (!val3)
        {
            this.error = true;
            this.errMsg = "Failed creating index";
        }
    }

    @Override
    public String getError()
    {
        return this.error ? this.errMsg : null;
    }

    @Override
    public String getStackTrace()
    {
        if (this.stackTrace == null || !this.error)
            return null;

        return Stream.of(this.stackTrace).map(StackTraceElement::toString).collect(Collectors.joining());
    }

    /**
     * Finds embeddings of entity
     * @param query is entity IRI string
     * @return list of doubles corresponding to entity embedding
     */
    @Override
    public List<Double> select(String query)
    {
        this.error = false;
        Integer id = getIriId(query.hashCode(), query);

        if (id == null)
        {
            this.error = true;
            this.errMsg = "Entity IRI does not exist";
            return null;
        }

        List<Double> embedding = getEmbedding(id);

        if (embedding == null)
        {
            this.error = true;
            this.errMsg = "Could not read embedding for entity IRI";
            return null;
        }

        return embedding;
    }

    private Integer getIriId(int hash, String iri)
    {
        ResultSet ids = this.driver.select("SELECT " + IRI_FIELD + ", " + ID_FIELD + " " +
                                                 "FROM " + HASHES + " " +
                                                 "WHERE " + HASH_FIELD + " = '" + hash + "';");

        try
        {
            while (ids.next())
            {
                if (ids.getString(1).equals(iri))
                    return ids.getInt(2);
            }

            throw new SQLException("IRI does not exist");
        }

        catch (SQLException exception)
        {
            this.error = true;
            this.errMsg = exception.getMessage();
            this.stackTrace = exception.getStackTrace();

            return null;
        }
    }

    private List<Double> getEmbedding(int id)
    {
        List<Double> vector = new ArrayList<>();
        ResultSet embedding = this.driver.select("SELECT " + EMBEDDING_FIELD + " " +
                                                       "FROM " + EMBEDDINGS + " " +
                                                       "WHERE " + ID_FIELD + " = '" + id + "';");

        try
        {
            if (!embedding.next())
                throw new SQLException("Could not find embedding from ID");

            String[] es = embedding.getString(1).split(",");

            for (String e : es)
            {
                vector.add(Double.parseDouble(e));
            }

            return vector;
        }

        catch (SQLException exception)
        {
            this.error = true;
            this.errMsg = exception.getMessage();
            this.stackTrace = exception.getStackTrace();

            return null;
        }
    }

    /**
     * Insertion of entity with embedding
     * @param query entity IRI and embedding separated by space and embedding values are separated by comma
     * @return true if insertion succeeds
     */
    @Override
    public boolean update(String query)
    {
        String[] split = query.split(" ");

        if (split.length != 2 || split[1].split(",").length < 2)
        {
            this.error = true;
            this.errMsg = "Query could not be parsed";
            return false;
        }

        String iri = split[0], embeddings = split[1];
        boolean val1 = this.driver.update("INSERT INTO " + HASHES + " (" + HASH_FIELD + ", " + IRI_FIELD + ") " +
                                                "VALUES ('" + iri.hashCode() + "', '" + iri + "');");

        if (!val1)
        {
            this.error = true;
            this.errMsg = ((ExplainableCause) this.driver).getError();
            return false;
        }

        Integer id = getIriId(iri.hashCode(), iri);

        if (id == null)
        {
            this.error = true;
            this.errMsg = "Could not read new IRI ID";
            return false;
        }

        boolean val2 = this.driver.update("INSERT INTO " + EMBEDDINGS + " (" + ID_FIELD + ", " + EMBEDDING_FIELD + ") " +
                                                "VALUES ('" + id + "', '" + embeddings + "');");

        if (!val2)
        {
            this.driver.update("DELETE FROM " + HASHES + " WHERE " + HASH_FIELD + " = '" + iri.hashCode() + "';");
            this.error = true;
            this.errMsg = ((ExplainableCause) this.driver).getError();
            return false;
        }

        return true;
    }

    @Override
    public boolean updateSchema(String query)
    {
        throw new UnsupportedOperationException("Schema cannot be updated");
    }

    @Override
    public boolean close()
    {
        return this.driver.close();
    }

    /**
     * Drops all relations
     * @param query is redundant
     * @return true if relations were dropped successfully
     */
    @Override
    public boolean drop(String query)
    {
        boolean val1 = this.driver.drop("DROP TABLE " + HASHES + ";"),
                val2 = this.driver.drop("DROP TABLE " + EMBEDDINGS + ";");

        return val1 && val2;
    }

    /**
     * Batch insertion of embeddings
     * @param iris List of entity IRIs
     * @param vectors List of embedding vectors
     * @return True if batch insertion was successfull
     */
    @Override
    public boolean batchInsert(List<String> iris, List<List<Float>> vectors)
    {
        if (iris.size() != vectors.size())
        {
            this.error = true;
            this.errMsg = "Number of IRIs and embedding vectors does not match";
            return false;
        }

        boolean val1 = batchInsertHash(iris),
                val2 = batchInsertEmbeddings(iris, vectors);

        return val1 && val2;
    }

    private boolean batchInsertHash(List<String> iris)
    {
        StringBuilder builder = new StringBuilder("INSERT INTO " + HASHES + " (" + HASH_FIELD + ", " + IRI_FIELD + ") VALUES ");
        int size = iris.size();

        for (int i = 0; i < size; i++)
        {
            builder.append("('")
                    .append(iris.get(i).hashCode())
                    .append("', '").append(iris.get(i).replace("'", "''"))
                    .append("'), ");
        }

        builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append(";");
        return this.driver.update(builder.toString());
    }

    private boolean batchInsertEmbeddings(List<String> iris, List<List<Float>> vectors)
    {
        StringBuilder builder = new StringBuilder("INSERT INTO " + EMBEDDINGS + " (" + ID_FIELD + ", " + EMBEDDING_FIELD + ") VALUES ");
        int size = iris.size();

        for (int i = 0; i < size; i++)
        {
            Integer id = getIriId(iris.get(i).hashCode(), iris.get(i));

            if (id == null)
                return false;

            builder.append("('")
                    .append(id)
                    .append("', '")
                    .append(vector2Str(vectors.get(i)))
                    .append("'), ");
        }

        builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append(";");
        return this.driver.update(builder.toString());
    }

    private static String vector2Str(List<Float> vector)
    {
        StringBuilder builder = new StringBuilder();
        vector.forEach(f -> builder.append(f.toString()).append(","));
        builder.deleteCharAt(builder.length() - 1);

        return builder.toString();
    }
}
