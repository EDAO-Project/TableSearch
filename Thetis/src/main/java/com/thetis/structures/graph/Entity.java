package com.thetis.structures.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents entity in a knowledge graph
 * Each entity has an IDF weight and a set of types
 */
public class Entity implements Comparable<Entity>, Serializable
{
    private String uri;
    private List<Type> types;
    private List<String> predicates;
    private double idf = 1;

    public Entity(String uri)
    {
        this(uri, List.of(), List.of());
    }

    public Entity(String uri, List<Type> types, List<String> predicates)
    {
        this.uri = uri;
        this.types = types;
        this.predicates = predicates;
    }

    public Entity(String uri, double idf, List<Type> types, List<String> predicates)
    {
        this(uri, types, predicates);
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

    public List<String> getPredicates()
    {
        return this.predicates;
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

    @Override
    public int compareTo(Entity o)
    {
        if (equals(o))
            return 0;

        return this.uri.compareTo(o.getUri());
    }
}
