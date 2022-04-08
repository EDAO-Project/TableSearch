package dk.aau.cs.daisy.edao.structures.graph;

import java.util.Arrays;
import java.util.List;

public class Entity
{
    private String uri;
    private List<Type> types;
    private double idf = -1;

    public Entity(String uri, Type ... types)
    {
        this(uri, Arrays.asList(types));
    }

    public Entity(String uri, List<Type> types)
    {
        this.uri = uri;
        this.types = types;
    }

    public Entity(String uri, double idf, Type ... types)
    {
        this(uri, types);
        this.idf = idf;
    }

    public Entity(String uri, double idf, List<Type> types)
    {
        this(uri, types);
        this.idf = idf;
    }

    public String getUri()
    {
        return this.uri;
    }

    public List<Type> getTypes()
    {
        return this.types;
    }

    public double getIDF()
    {
        return this.idf;
    }

    public void setIDF(double idf)
    {
        this.idf = idf;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Entity))
            return false;

        Entity other = (Entity) o;

        return this.uri.equals(other.uri) && this.types.equals(other.types);
    }
}