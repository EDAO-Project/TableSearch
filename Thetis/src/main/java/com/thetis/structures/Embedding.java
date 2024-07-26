package com.thetis.structures;

import com.thetis.similarity.CosineSimilarity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Embedding implements Comparable<Embedding>, Serializable
{
    private final List<Double> embedding;
    private final int dimension;

    public Embedding(List<Double> embedding)
    {
        this.embedding = new ArrayList<>(embedding);
        this.dimension = embedding.size();
    }

    public List<Double> toList()
    {
        return this.embedding;
    }

    public int getDimension()
    {
        return this.dimension;
    }

    public double cosine(Embedding other)
    {
        return CosineSimilarity.make(this.embedding, other.toList()).similarity();
    }

    @Override
    public int compareTo(Embedding e)
    {
        double cosine = cosine(e);
        return Double.compare(0.0, cosine);
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Embedding))
            return false;

        Embedding o = (Embedding) other;
        return this.embedding.equals(o.embedding) && this.dimension == o.dimension;
    }
}
