package dk.aau.cs.daisy.edao.connector;

import io.milvus.grpc.DataType;
import io.milvus.grpc.QueryResults;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.QueryResultsWrapper;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage of embedding in a Milvus instance
 * SQLite is used to bi-directional mapping between entityIRIs and IDs
 */
public class EmbeddingStore implements DBDriverEmbedding<List<Double>, String>
{
    private Milvus milvus;
    private SQLite sqlite;
    private int embeddingDimension;
    private static final String sqliteName = "embeddings.db";
    private static final String tableName = "Mapping";
    private static final String collectionName = "embeddings";

    public EmbeddingStore(String dbPath, String milvusHost, int milvusPort, int embeddingDimension)
    {
        if (embeddingDimension < 1)
            throw new IllegalArgumentException("Embedding dimension must be at least 1");

        this.sqlite = SQLite.init(sqliteName, dbPath);
        this.milvus = new Milvus(milvusHost, milvusPort);
        this.embeddingDimension = embeddingDimension;
        setup();
    }

    private void setup()
    {
        drop(null);
        setupSqlite();
        setupMilvus();
    }

    private void setupSqlite()
    {
        boolean val = this.sqlite.updateSchema("CREATE TABLE IF NOT EXISTS " + tableName + " " +
                                                            "(id INTEGER PRIMARY KEY AUTOINCREMENT," +
                                                            "iri VARCHAR(100) NOT NULL);");

        if (!val)
            throw new RuntimeException("Could not setup SQLite table");
    }

    private void setupMilvus()
    {
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.CREATE, collectionName);
        FieldType id = FieldType.newBuilder()
                            .withName("id")
                            .withDataType(DataType.Int64)
                            .withPrimaryKey(true)
                            .build();
        FieldType embedding = FieldType.newBuilder()
                            .withName("embedding")
                            .withDataType(DataType.FloatVector)
                            .withDimension(this.embeddingDimension)
                            .build();
        command.addProperty(id);
        command.addProperty(embedding);
        this.milvus.updateSchema(command);
    }

    /**
     * Search for entity embeddings
     * @param query is an entity
     * @return embeddings of given entity
     * @throws {@code IllegalArgumentException} if there is no ID for the given entity
     */
    @Override
    public List<Double> select(String query)
    {
        long iriId = getIriId(query);

        if (iriId == -1)
            throw new IllegalArgumentException("Query entity does not exist");

        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.QUERY, collectionName);
        command.addProperty("id", "variable");
        command.addProperty("embedding", "variable");
        command.addProperty("id == " + iriId, "expr");

        QueryResults results = this.milvus.select(command);
        return ((List<List<Double>>) new QueryResultsWrapper(results).
                getFieldWrapper("embedding").getFieldData()).get(0);
    }

    /**
     * Insertion of a single embedding vector
     * @param query is an entity and embedding vector seperated by space ' '
     *              Embeddings vector values are seperated by comma ','
     * @return
     */
    @Override
    public boolean update(String query)
    {
        String[] qSplit = query.split(" ");

        if (qSplit.length != 2)
            throw new IllegalArgumentException("Query must be IRI and embeddings vector seperated by space ' '\n" +
                    "Make sure there are no other spaces in query");

        String iri = qSplit[0];
        String[] embeddings = qSplit[1].split(",");
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.UPDATE, collectionName);

        if (embeddings.length == 0)
            throw new IllegalArgumentException("There are no embeddings in query");

        boolean ret1 = this.sqlite.update("INSERT INTO " + tableName + " (iri) VALUES ('" + iri + "');");
        InsertParam.Field idField = new InsertParam.Field("id", DataType.Int64, List.of(getIriId(iri))),
                embeddingsField = new InsertParam.Field("embedding", DataType.FloatVector, List.of(milvusEmbedding(embeddings)));
        command.addProperty(idField);
        command.addProperty(embeddingsField);

        boolean ret2 = this.milvus.update(command);
        return ret1 && ret2;
    }

    private static List<Float> milvusEmbedding(String[] embeddings)
    {
        List<Float> vector = new ArrayList<>(1);

        for (String e : embeddings)
        {
            vector.add(Float.parseFloat(e));
        }

        return vector;
    }

    private long getIriId(String iri)
    {
        ResultSet rs = this.sqlite.select("SELECT id FROM " + tableName + " WHERE iri LIKE '" + iri + "';");

        if (rs == null)
            return -1;

        try
        {
            if (!rs.next())
                return -1;

            return rs.getInt(1);
        }

        catch (SQLException exception)
        {
            return -1;
        }
    }

    /**
     * Batch insertion
     * @param iris List of IRIs
     * @param vectors List of vectors
     * @return True if batch insertion succeeded
     */
    @Override
    public boolean batchInsert(List<String> iris, List<List<Float>> vectors)
    {
        List<Long> ids = addIris(iris);
        InsertParam.Field idField = new InsertParam.Field("id", DataType.Int64, ids),
                embeddingField = new InsertParam.Field("embedding", DataType.FloatVector, vectors);
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.UPDATE, collectionName);
        command.addProperty(idField);
        command.addProperty(embeddingField);

        return this.milvus.update(command);
    }

    private List<Long> addIris(List<String> iris)
    {
        List<Long> ids = new ArrayList<>();

        for (String iri : iris)
        {
            this.sqlite.update("INSERT INTO " + tableName + " (iri) VALUES ('" + iri + "');");
            ids.add(getIriId(iri));
        }

        return ids;
    }

    /**
     * Updating or creating schema is not possible since this class handles it
     * @param query
     * @return Throws {@code UnsupportedOperationException}
     */
    @Override
    public boolean updateSchema(String query)
    {
        throw new UnsupportedOperationException("Schema update is not supported");
    }

    /**
     * Closes connection to SQLite and Milvus
     * This class is not usable after calling this method
     * @return {@code true} if both SQLite and Milvus could be closed
     */
    @Override
    public boolean close()
    {
        boolean val1 = this.milvus.close();
        boolean val2 = this.sqlite.close();
        return val1 && val2;
    }

    /**
     * Drops SQLite table and Milvus collection
     * @param query is redundant
     * @return {@code true} if both the table in SQLite and the collection in Milvus could be dropped
     */
    @Override
    public boolean drop(String query)
    {
        return this.milvus.drop(Milvus.createCommand(MilvusCommand.Type.UPDATE, collectionName));
    }
}
