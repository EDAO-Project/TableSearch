package com.thetis.structures.graph;

import java.io.Serializable;

/**
 * Represents a knowledge graph entity type
 * It also contains its IDF weight
 */
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
        return this.type;
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
     * @param o Object to compare equality against
     * @return True if the object is equal by string representation and IDF score
     */
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

        else if (this.type.equals(o.getType()))
            return this.idf < o.idf ? -1 : 1;

        return type.compareTo(o.getType());
    }
}
