package com.thetis.connector;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class should only be used to populate and search, since deletion is not supported
 */
public class Milvus implements DBDriver<QueryResults, MilvusCommand>, ExplainableCause
{
    private MilvusServiceClient client;
    private boolean error = false;
    private String errorMsg;
    private StackTraceElement[] stackTrace;

    public Milvus(String host, int port)
    {
        this.client = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(port)
                        .build()
        );
    }

    public static MilvusCommand createCommand(MilvusCommand.Type type, String collectionName)
    {
        return MilvusCommand.create(type, collectionName);
    }

    @Override
    public boolean close()
    {
        this.client.close();
        return true;
    }

    /**
     * Searching Milvus
     * @param query Command of properties to be used in searching
     *              Property of type String with "expr" as value to become string expression
     *              Properties of type String with "variable" as value become query output fields
     * @return ResultSet of search output
     */
    @Override
    public QueryResults select(MilvusCommand query)
    {
        if (query.getType() != MilvusCommand.Type.QUERY)
            throw new IllegalArgumentException("Milvus command must be of type QUERY");

        int status = this.client.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(query.getCollectionName())
                        .build()
        ).getStatus();

        if (status != R.Status.Success.getCode())
        {
            this.error = true;
            this.errorMsg = "Failed loading collection";
            return null;
        }

        QueryParam.Builder search = QueryParam.newBuilder().withCollectionName(query.getCollectionName());
        String expr = getExpr(query);
        List<String> outputFields = getOutputFields(query);

        if (expr != null)
            search.withExpr(expr);

        if (!outputFields.isEmpty())
            search.withOutFields(outputFields);

        R<QueryResults> results = this.client.query(search.build());

        this.client.releaseCollection(
                ReleaseCollectionParam.newBuilder()
                        .withCollectionName(query.getCollectionName())
                        .build()
        );

        return results.getData();
    }

    private static String getExpr(MilvusCommand command)
    {
        for (Map.Entry<Object, String> entry : command.properties())
        {
            if (entry.getKey() instanceof String && entry.getValue().equals("expr"))
                return (String) entry.getKey();
        }

        return null;
    }

    private static List<String> getOutputFields(MilvusCommand command)
    {
        List<String> fields = new ArrayList<>();

        for (Map.Entry<Object, String> entry : command.properties())
        {
            if (entry.getKey() instanceof String && entry.getValue().equals("variable"))
                fields.add((String) entry.getKey());
        }

        return fields;
    }

    /**
     * Used to insert elements only
     * @param query collection of Fields to be added to existing collection
     * @return Boolean depending on whether adding fields was successful
     */
    @Override
    public boolean update(MilvusCommand query)
    {
        if (query.getType() != MilvusCommand.Type.UPDATE)
            throw new IllegalArgumentException("Milvus command must be of type UPDATE");

        Set<Map.Entry<Object, String>> properties = query.properties();
        List<InsertParam.Field> fields = new ArrayList<>(properties.size());
        InsertParam.Builder builder = InsertParam.newBuilder()
                .withCollectionName(query.getCollectionName());

        for (Map.Entry<Object, String> property : properties)
        {
            if (!(property.getKey() instanceof InsertParam.Field))
            {
                this.error = true;
                this.errorMsg = "A command property is not of type 'InsertParam.Field'";
                return false;
            }

            fields.add((InsertParam.Field) property.getKey());
        }

        InsertParam insert = builder.withFields(fields).build();
        return this.client.insert(insert).getStatus() == R.Status.Success.getCode();
    }

    /**
     * Can only create a new collection, not modify
     * @param query collection of FieldTypes to be added to collection
     * @return Boolean depending on whether the creation was successful
     */
    @Override
    public boolean updateSchema(MilvusCommand query)
    {
        if (query.getType() != MilvusCommand.Type.CREATE)
            throw new IllegalArgumentException("Milvus command must be of type CREATE");

        CreateCollectionParam.Builder builder = CreateCollectionParam.newBuilder()
                .withCollectionName(query.getCollectionName());
        Set<Map.Entry<Object, String>> properties = query.properties();

        for (Map.Entry<Object, String> property : properties)
        {
            if (!(property.getKey() instanceof FieldType))
            {
                this.error = true;
                this.errorMsg = "A command property is not of type 'FieldType'";
                return false;
            }

            builder.addFieldType((FieldType) property.getKey());
        }

        return this.client.createCollection(builder.build()).getStatus() == R.Status.Success.getCode();
    }

    /**
     * Dropping of collection
     * @param query Name of command is the name of the collection to be dropped
     */
    @Override
    public boolean drop(MilvusCommand query)
    {
        return this.client.dropCollection(
                DropCollectionParam.newBuilder()
                        .withCollectionName(query.getCollectionName())
                        .build()
        ).getStatus() == R.Status.Success.getCode();
    }

    /**
     * Creates index to speed up searching
     * @param collectionName Name of collection to create an index for
     * @param fieldName Field to function as index key
     * @param indexType Type of index
     * @param metricType Type of search metric
     * @return
     */
    public boolean createIndex(String collectionName, String fieldName, IndexType indexType, MetricType metricType)
    {
        return this.client.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withFieldName(fieldName)
                        .withIndexType(indexType)
                        .withMetricType(metricType)
                        .withSyncMode(true)
                        .build()
        ).getStatus() == R.Status.Success.getCode();
    }

    @Override
    public String getError()
    {
        return this.error ? this.errorMsg : null;
    }

    @Override
    public String getStackTrace()
    {
        if (!this.error)
            return null;

        StringBuilder builder = new StringBuilder();

        for (StackTraceElement elem : this.stackTrace)
        {
            builder.append(elem.toString()).append("\n");
        }

        return builder.toString();
    }
}
