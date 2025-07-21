package com.thetis.structures.graph;

import java.io.Serializable;

/**
 * Represents a knowledge graph entity type
 * It also contains its IDF weight
 */
public class Type implements Comparable<Type>, Serializable
{
    private String type;

    public Type(String type)
    {
        this.type = type;
    }

    @Override
    public String toString()
    {
        return this.type;
    }

    public String getType()
    {
        return this.type;
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
        return this.type.equals(other.type);
    }

    @Override
    public int compareTo(Type o)
    {
        if (equals(o))
            return 0;

        return type.compareTo(o.getType());
    }
}
