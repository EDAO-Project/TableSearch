package dk.aau.cs.daisy.edao.structures.graph;

import java.io.Serializable;

public class Type implements Comparable<Type>, Serializable
{
    private String type;
    private double idf = -1;

    public Type(String type)
    {
        this.type = type;
    }

    public Type(String type, double idf)
    {
        this(type);
        this.idf = idf;
    }

    @Override
    public String toString()
    {
        return this.type;
    }

    public String getType()
    {
        return toString();
    }

    public double getIdf()
    {
        return this.idf;
    }

    public void setIdf(double idf)
    {
        this.idf = idf;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Type))
            return false;

        Type other = (Type) o;
        return this.type.equals(other.type) && this.idf == other.idf;
    }

    @Override
    public int compareTo(Type o)
    {
        if (equals(o))
            return 0;

        return type.compareTo(o.getType());
    }
}
