package dk.aau.cs.daisy.edao.connector;

import io.milvus.grpc.DataType;
import io.milvus.grpc.QueryResults;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.QueryResultsWrapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MilvusTest
{
    private Milvus db;

    @Before
    public void load()
    {
        this.db = new Milvus("localhost", 19530);
        FieldType field1 = FieldType.newBuilder()
                .withName("field1")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .build();
        FieldType field2 = FieldType.newBuilder()
                .withName("field2")
                .withDataType(DataType.FloatVector)
                .withDimension(5)
                .build();
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.CREATE, "collection");
        command.addProperty(field1);
        command.addProperty(field2);

        this.db.updateSchema(command);
        insertData();
    }

    @After
    public void drop()
    {
        this.db.drop(Milvus.createCommand(MilvusCommand.Type.UPDATE, "collection"));
        this.db.close();
    }

    private boolean insertData()
    {
        List<Long> ids = List.of((long) 1, (long) 2);
        List<List<Float>> embeddings = List.of(List.of(1f, 2f, 3f, 4f, 5f), List.of(6f, 7f, 8f, 9f, 10f));
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.UPDATE, "collection");
        command.addProperty(new InsertParam.Field("field1", DataType.Int64, ids));
        command.addProperty(new InsertParam.Field("field2", DataType.FloatVector, embeddings));
        return this.db.update(command);
    }

    @Test
    public void readDataTest()
    {
        MilvusCommand command = Milvus.createCommand(MilvusCommand.Type.QUERY, "collection");
        command.addProperty("field1", "variable");
        command.addProperty("field2", "variable");
        command.addProperty("field1 in [1,2]", "expr");

        QueryResults qRes = this.db.select(command);
        Assert.assertNotNull(qRes);

        QueryResultsWrapper res = new QueryResultsWrapper(qRes);
        List<Long> ids = (List<Long>) res.getFieldWrapper("field1").getFieldData();
        List<List<Float>> embeddings = (List<List<Float>>) res.getFieldWrapper("field2").getFieldData();

        assertEquals(2, ids.size());
        assertEquals(2, embeddings.size());
        assertEquals(5, embeddings.get(0).size());
        assertEquals(5, embeddings.get(1).size());
    }
}
