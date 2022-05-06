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
        return this.type + " - " + this.idf;
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

    /**
     * Equality between type and object
     * (Important!) This is simple string comparison, which is a hacky way to fix a bug
     * @param o Object to compare equality against
     * @return True if the object if equal by string representation
     */
    @Override
    public boolean equals(Object o)
    {
        return toString().equals(o.toString());
    }

    @Override
    public int compareTo(Type o)
    {
        if (equals(o))
            return 0;

        return type.compareTo(o.getType());
    }
}
