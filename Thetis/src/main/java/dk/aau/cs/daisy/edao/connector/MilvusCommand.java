package dk.aau.cs.daisy.edao.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class MilvusCommand
{
    public enum Type {QUERY, UPDATE, CREATE}

    private Type type;
    private String name;
    private Map<Object, String> properties = new HashMap<>();

    public static MilvusCommand create(MilvusCommand.Type type, String collectionName)
    {
        return new MilvusCommand(type, collectionName);
    }

    private MilvusCommand(Type type, String collectionName)
    {
        this.type = type;
        this.name = collectionName;
    }

    public Type getType()
    {
        return this.type;
    }

    public String getCollectionName()
    {
        return this.name;
    }

    public MilvusCommand addProperty(Object obj, String name)
    {
        this.properties.put(obj, name);
        return this;
    }

    public MilvusCommand addProperty(Object obj)
    {
        this.properties.put(obj, null);
        return this;
    }

    public Set<Map.Entry<Object, String>> properties()
    {
        return this.properties.entrySet();
    }
}
